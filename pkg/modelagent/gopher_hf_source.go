package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/storage"
	"sigs.k8s.io/ome/pkg/xet"
)

// Dependencies are scoped to one source invocation; tests never replace global
// HTTP clients or the process-wide Xet client.
type directHfSource struct {
	resolve  func(context.Context, string, string, string, string) (string, error)
	manifest func(context.Context, string, string, string, string) (hfSnapshotManifest, error)
	download func(context.Context, *GopherTask, *xet.DownloadConfig) error
}

func (s *Gopher) processDirectHfModel(ctx context.Context, task *GopherTask, spec v1beta1.BaseModelSpec, allowDownload bool) (bool, error) {
	source := directHfSource{resolve: resolveHfRevision, manifest: fetchHfSnapshotManifest, download: s.downloadDirectHfSnapshot}
	return source.process(ctx, s, task, spec, allowDownload)
}

func (source directHfSource) process(ctx context.Context, s *Gopher, task *GopherTask, spec v1beta1.BaseModelSpec, allowDownload bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if task == nil || (task.BaseModel == nil) == (task.ClusterBaseModel == nil) ||
		(task.TaskType != Download && task.TaskType != DownloadOverride) ||
		spec.Storage == nil || spec.Storage.StorageUri == nil {
		return false, fmt.Errorf("direct HF download requires a model task and URI")
	}
	components, err := storage.ParseHuggingFaceStorageURI(*spec.Storage.StorageUri)
	if err != nil {
		return false, err
	}
	if !isValidHfModelID(components.ModelID) {
		return false, fmt.Errorf("invalid direct HF model ID %q", components.ModelID)
	}
	// Preserve the ordinary downloader's empty-path fallback without mutating
	// the task's spec. A missing path is not eligible for canonical-parent reuse.
	storageSpec := *spec.Storage
	spec.Storage = &storageSpec
	if spec.Storage.Path == nil {
		emptyPath := ""
		spec.Storage.Path = &emptyPath
	}
	destination := getDestPath(&spec, s.modelRootDir)
	if filepath.Clean(destination) == string(filepath.Separator) {
		return false, fmt.Errorf("direct HF destination cannot be the filesystem root")
	}
	for _, part := range strings.Split(destination, string(filepath.Separator)) {
		if part == constants.ModelArtifactsDirectory {
			return false, fmt.Errorf("direct HF child path cannot be inside the shared artifact directory")
		}
	}
	if _, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, allowDownload); waiting || err != nil {
		return waiting, err
	}
	config := &xet.DownloadConfig{}
	if s.xetConfig != nil {
		config = s.xetConfig.ToDownloadConfig()
	}
	config.RepoID, config.LocalDir = components.ModelID, destination
	if components.Branch != "" {
		config.Revision = components.Branch
	}
	if token := s.getHuggingFaceToken(ctx, task, spec, getModelInfoForLogging(task)); token != "" {
		config.Token = token
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	resolved := task.HfResolvedRevision
	if resolved == "" {
		resolved, err = source.resolve(ctx, config.RepoID, config.Revision, config.Token, config.Endpoint)
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if err != nil {
			resolved = ""
		}
		if resolved == "" {
			if err == nil {
				err = fmt.Errorf("HF revision resolution returned no immutable revision for %s", config.RepoID)
			}
			// Unknown identity is not an identity change. Keep an existing shared
			// copy attached until resolution can authorize any replacement.
			if s.configMapReconciler != nil {
				key := getModelID(task.BaseModel, task.ClusterBaseModel)
				parent, referenced, lookupErr := s.sharedHfArtifactHandler().repository.GetParentForChild(ctx, key)
				if ctx.Err() != nil {
					return false, ctx.Err()
				}
				if lookupErr != nil && !apierrors.IsNotFound(lookupErr) {
					return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(key, lookupErr))
				}
				if referenced {
					return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(parent.Key, err))
				}
			}
			// Fresh ordinary downloads still use best-effort revision resolution.
			s.logger.Warnf("Cannot resolve HF revision for %s; using ordinary download: %v", config.RepoID, err)
		}
	}
	var input hfArtifactTaskInput
	var eligible bool
	if resolved != "" {
		identity, identityErr := newHfArtifactIdentity(config.RepoID, resolved)
		if identityErr != nil {
			return false, identityErr
		}
		task.HfResolvedRevision = identity.CommitSHA
		config.Revision = identity.CommitSHA
		if isDirectHfReuseEligible(task, spec.Storage) && s.configMapReconciler != nil {
			input, eligible, err = newHfArtifactTaskInput(task, spec.Storage, s.modelRootDir, identity)
			if err != nil {
				return false, err
			}
		}
	}
	if waiting, err := s.detachChangedDirectHfReference(ctx, task, spec, input, eligible, allowDownload); waiting || err != nil {
		return waiting, err
	}
	if eligible {
		// Fetch at most once in this attempt, only if shared validation or a
		// download needs it. Ordinary ready reuse requires no extra Hub call.
		var manifest hfSnapshotManifest
		var manifestErr error
		var manifestOnce sync.Once
		getManifest := func() (hfSnapshotManifest, error) {
			manifestOnce.Do(func() {
				manifest, manifestErr = source.manifest(ctx, config.RepoID, config.Revision, config.Token, config.Endpoint)
				if manifestErr == nil {
					manifestErr = manifest.check(config.Revision)
				}
			})
			return manifest, manifestErr
		}
		validate := func(parentPath string) (bool, error) {
			manifest, err := getManifest()
			if err != nil {
				return false, err
			}
			return manifest.validate(ctx, parentPath)
		}
		download := func(parentPath string) error {
			manifest, err := getManifest()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(parentPath, 0o755); err != nil {
				return err
			}
			// The shared handler owns the parent and has failed all children
			// before reaching this callback. Xet otherwise skips same-size damage.
			if err := manifest.removeInvalidFiles(ctx, parentPath); err != nil {
				return err
			}
			parentConfig := *config
			parentConfig.LocalDir = parentPath
			if err := source.download(ctx, task, &parentConfig); err != nil {
				return err
			}
			valid, err := manifest.validate(ctx, parentPath)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("downloaded HF snapshot %s@%s failed content validation", config.RepoID, config.Revision)
			}
			return nil
		}
		result, err := s.runHfArtifactDownload(ctx, task, input, allowDownload, validate, download)
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if err != nil {
			return false, err
		}
		switch result.Outcome {
		case hfArtifactTaskRetry:
			return true, s.requeueHfArtifactTask(task, result)
		case hfArtifactTaskDone:
			if !allowDownload && task.NormalPriorityOnly {
				return true, nil
			}
			return false, s.parseDirectHfConfig(ctx, task, destination, nil)
		case hfArtifactTaskUseDefaultDownload:
			// Real legacy directories remain ordinary directories. In particular
			// do not adopt them as canonical parents or remove their descendants.
			if waiting, err := s.detachHfArtifactForDefaultDownload(ctx, task, spec, allowDownload); waiting || err != nil {
				return waiting, err
			}
		default:
			return false, fmt.Errorf("unexpected shared HF task outcome %q", result.Outcome)
		}
	}
	if isSharedHfArtifactSymlink(destination) {
		return false, fmt.Errorf("direct HF destination %s is a shared link without a usable persisted reference", destination)
	}
	if !allowDownload {
		s.demoteToNormalPriority(task)
		return true, nil
	}
	unlock, acquired, err := s.acquireDirectArtifactDownload(ctx, task)
	if err != nil || !acquired {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(gopherTaskModelKey(task), err))
	}
	defer unlock()
	if err := checkDirectHfDestinationAncestors(destination); err != nil {
		return false, err
	}
	if handled, err := source.reuseLegacyDescendants(ctx, s, task, spec, config); handled || err != nil {
		return false, err
	}
	artifact, err := s.directHfLegacyArtifact(ctx, task, destination, task.HfResolvedRevision)
	if err != nil {
		return false, err
	}
	if len(artifact.ChildrenPaths) != 0 {
		return false, fmt.Errorf("legacy HF artifact has descendants; use a different destination before replacing its contents")
	}
	if err := source.download(ctx, task, config); err != nil {
		return false, err
	}
	return false, s.parseDirectHfConfig(ctx, task, destination, artifact)
}

// Matching shared children stay attached. Only a policy/identity/path change
// goes through the ordinary-download detach path.
func (s *Gopher) detachChangedDirectHfReference(ctx context.Context, task *GopherTask, spec v1beta1.BaseModelSpec, input hfArtifactTaskInput, eligible, allowDownload bool) (bool, error) {
	if eligible {
		parent, referenced, err := s.sharedHfArtifactHandler().repository.GetParentForChild(ctx, input.ChildModelKey)
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if !referenced || (parent.Key == input.Parent.Key && parent.Children[input.ChildModelKey] == input.ChildModelPath) {
			return false, nil
		}
	}
	return s.detachHfArtifactForDefaultDownload(ctx, task, spec, allowDownload)
}

// Retain legacy artifact metadata so existing deletion code still accounts for
// descendant links. Unlink only the child itself, never its former real parent.
func (s *Gopher) directHfLegacyArtifact(ctx context.Context, task *GopherTask, destination, revision string) (*Artifact, error) {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	old, err := s.readDirectHfLegacyArtifact(ctx, task)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(destination)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		if len(old.ChildrenPaths) != 0 {
			return nil, fmt.Errorf("legacy HF artifact has descendants; refusing to replace its link")
		}
		if isSharedHfArtifactSymlink(destination) {
			return nil, fmt.Errorf("refusing legacy write through shared HF link %s", destination)
		}
		if err := os.Remove(destination); err != nil {
			return nil, err
		}
	}
	parent, _ := parseParent(old.ParentPath)
	if s.configMapReconciler != nil && parent != "" && parent != key && len(old.ChildrenPaths) == 0 {
		if err := s.configMapReconciler.updateConfigMapWithRemovedChildPath(ctx, parent, destination); err != nil {
			return nil, err
		}
	}
	return &Artifact{Sha: revision, ParentPath: map[string]string{key: destination}, ChildrenPaths: old.ChildrenPaths}, nil
}

func (s *Gopher) readDirectHfLegacyArtifact(ctx context.Context, task *GopherTask) (Artifact, error) {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	if s.configMapReconciler != nil {
		configMap, err := s.configMapReconciler.getConfigMap(ctx)
		if apierrors.IsNotFound(err) {
			return Artifact{}, nil
		}
		if err != nil {
			return Artifact{}, err
		}
		if data, exists := configMap.Data[key]; exists {
			var entry ModelEntry
			if err := json.Unmarshal([]byte(data), &entry); err != nil {
				return Artifact{}, fmt.Errorf("read legacy HF artifact %s: %w", key, err)
			}
			if entry.Config != nil {
				return entry.Config.Artifact, nil
			}
		}
	}
	return Artifact{}, nil
}

func (source directHfSource) reuseLegacyDescendants(ctx context.Context, s *Gopher, task *GopherTask, spec v1beta1.BaseModelSpec, config *xet.DownloadConfig) (bool, error) {
	old, err := s.readDirectHfLegacyArtifact(ctx, task)
	if err != nil || len(old.ChildrenPaths) == 0 {
		return false, err
	}
	if spec.Storage.DownloadPolicy == nil || *spec.Storage.DownloadPolicy != v1beta1.ReuseIfExists ||
		task.HfResolvedRevision == "" || !strings.EqualFold(old.Sha, task.HfResolvedRevision) {
		return true, fmt.Errorf("legacy HF artifact has descendants; use a different destination before changing its source or policy")
	}
	manifest, err := source.manifest(ctx, config.RepoID, config.Revision, config.Token, config.Endpoint)
	if err == nil {
		err = manifest.check(config.Revision)
	}
	var valid bool
	if err == nil {
		var directory string
		directory, err = filepath.EvalSymlinks(config.LocalDir)
		if err == nil {
			valid, err = manifest.validate(ctx, directory)
		}
	}
	if err != nil {
		return true, fmt.Errorf("legacy HF artifact has descendants and cannot be validated: %w", err)
	}
	if !valid {
		return true, fmt.Errorf("legacy HF artifact has descendants and requires repair; use a different destination")
	}
	// Legacy descendants have no shared-parent repair/status contract. Healthy
	// reuse is read-only; never adopt or repair their parent directory in place.
	return true, s.parseDirectHfConfig(ctx, task, config.LocalDir, &old)
}

func checkDirectHfDestinationAncestors(destination string) error {
	// A child may be ordinary while an ancestor resolves into a shared parent.
	// Resolve the nearest existing directory without creating anything first.
	for dir := filepath.Dir(destination); ; dir = filepath.Dir(dir) {
		resolved, err := filepath.EvalSymlinks(dir)
		if err == nil {
			for _, part := range strings.Split(resolved, string(filepath.Separator)) {
				if part == constants.ModelArtifactsDirectory {
					return fmt.Errorf("direct HF destination %s resolves inside a shared artifact", destination)
				}
			}
			return nil
		}
		if !os.IsNotExist(err) || dir == filepath.Dir(dir) {
			return err
		}
	}
}

func (s *Gopher) parseDirectHfConfig(ctx context.Context, task *GopherTask, destination string, artifact *Artifact) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.requiresArtifactRequestValidation(ctx, task) {
		if err := s.validateArtifactDownload(ctx, task); err != nil {
			return err
		}
	}
	if s.modelConfigParser != nil {
		s.logger.Debugf("Using %s for config parsing", getModelInfoForLogging(task))
		if err := s.safeParseAndUpdateModelConfig(ctx, destination, task.BaseModel, task.ClusterBaseModel, artifact); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.logger.Warnf("Failed to parse direct HF model config at %s: %v", destination, err)
		}
	}
	return ctx.Err()
}

func (s *Gopher) downloadDirectHfSnapshot(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
	return downloadDirectHfSnapshotWithProgress(ctx, config, xet.SnapshotDownloadWithProgress, func(flushCtx context.Context, progress *DownloadProgress) {
		s.updateDirectHfProgress(flushCtx, task, progress)
	})
}

func (s *Gopher) updateDirectHfProgress(ctx context.Context, task *GopherTask, progress *DownloadProgress) {
	if s.configMapReconciler == nil {
		return
	}
	// Finish an active write before cleanup, or reject a canceled writer that
	// waited behind cleanup. A valid CR UID alone cannot guard affinity changes.
	s.configMapMutex.Lock()
	defer s.configMapMutex.Unlock()
	if ctx.Err() != nil {
		return
	}
	op := &ConfigMapProgressOp{Progress: progress, BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel}
	if s.requiresArtifactRequestValidation(ctx, task) {
		op.validateCurrent = func() error { return s.validateArtifactDownload(ctx, task) }
	}
	if err := s.configMapReconciler.ReconcileModelProgress(ctx, op); err != nil {
		s.logger.Warnf("Failed to update direct HF download progress: %v", err)
	}
}

func downloadDirectHfSnapshotWithProgress(ctx context.Context, config *xet.DownloadConfig,
	download func(context.Context, *xet.DownloadConfig, xet.ProgressHandler, time.Duration) (string, error),
	flush func(context.Context, *DownloadProgress),
) error {
	const interval = 30 * time.Second
	const flushTimeout = 5 * time.Second
	var latest atomic.Pointer[DownloadProgress]
	var progressMu sync.Mutex
	lastBytes, lastTime := uint64(0), time.Now()
	stop, done := make(chan struct{}), make(chan struct{})
	flushLatest := func() {
		if progress := latest.Swap(nil); progress != nil {
			flushCtx, cancel := context.WithTimeout(ctx, flushTimeout)
			defer cancel()
			flush(flushCtx, progress)
		}
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				flushLatest()
			case <-stop:
				flushLatest()
				return
			}
		}
	}()
	stopProgress := sync.OnceFunc(func() { close(stop); <-done })
	defer stopProgress()
	// The Rust adapter inserts revision directly into tree/resolve URL paths.
	// Escape here, not before resolving or storing the task's immutable identity.
	xetConfig := *config
	xetConfig.Revision = url.PathEscape(config.Revision)
	_, err := download(ctx, &xetConfig, func(update xet.ProgressUpdate) {
		progressMu.Lock()
		defer progressMu.Unlock()
		now := time.Now()
		var speed float64
		if elapsed := now.Sub(lastTime).Seconds(); elapsed > 0 && update.CompletedBytes > lastBytes {
			speed = float64(update.CompletedBytes-lastBytes) / elapsed
		}
		lastBytes, lastTime = update.CompletedBytes, now
		latest.Store(&DownloadProgress{Phase: update.Phase.String(), TotalBytes: update.TotalBytes, CompletedBytes: update.CompletedBytes,
			TotalFiles: update.TotalFiles, CompletedFiles: update.CompletedFiles, SpeedBytesPerSec: speed, LastUpdated: now.Format(time.RFC3339)})
	}, interval)
	// Join before a terminal status clears progress, and observe cancellation
	// that occurred while the final flush was waiting for the ConfigMap lock.
	stopProgress()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
