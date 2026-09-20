package modelagent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/utils"
)

// processHfOCIArtifact leaves ineligible CRs on the existing OCI download path.
// handled distinguishes that fallback from a completed shared-artifact task;
// waiting prevents deferred work from being published as Ready by Gopher.
func (s *Gopher) processHfOCIArtifact(ctx context.Context, task *GopherTask, spec v1beta1.BaseModelSpec, allowDownload bool) (handled, waiting bool, err error) {
	if s.isOrdinaryArtifactTask(task) {
		return false, false, nil
	}
	input, eligible, err := newHfArtifactTaskInputForOCI(task, spec.Storage, s.modelRootDir)
	if err != nil {
		return true, false, err
	}
	if s.configMapReconciler == nil {
		return false, false, nil
	}
	handler := s.sharedHfArtifactHandler()
	key := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
	stored, referenced, lookupErr := handler.repository.GetParentForChild(ctx, key)
	if lookupErr != nil && !apierrors.IsNotFound(lookupErr) {
		// Opt-out still needs the old relationship for cleanup, even when the
		// replacement path is not a shared symlink.
		if task.SharedArtifact || eligible || isSharedHfArtifactSymlink(getDestPath(&spec, s.modelRootDir)) {
			return true, true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(key, lookupErr))
		}
		return false, false, nil
	}
	if referenced && (!eligible || stored.Key != input.Parent.Key || filepath.Clean(stored.Children[key]) != filepath.Clean(input.ChildModelPath)) {
		if !allowDownload {
			s.demoteToNormalPriority(task)
			return true, true, nil
		}
		oldInput := s.hfArtifactInputForChild(task, stored)
		preserve, lookupErr := s.isPathReferencedByOtherModels(oldInput.ChildModelPath, task.BaseModel, task.ClusterBaseModel)
		if lookupErr != nil {
			return true, false, lookupErr
		}
		if preserve && filepath.Clean(oldInput.ChildModelPath) == filepath.Clean(getDestPath(&spec, s.modelRootDir)) {
			return true, false, fmt.Errorf("cannot replace child path %s still used by another model", oldInput.ChildModelPath)
		}
		result, deleteErr := s.releaseHfArtifactChild(ctx, oldInput, preserve)
		if deleteErr != nil {
			return true, false, deleteErr
		}
		if result.Outcome == hfArtifactTaskRetry {
			return true, true, s.requeueHfArtifactTask(task, result)
		}
	}
	if !eligible {
		if !referenced && isSharedHfArtifactSymlink(getDestPath(&spec, s.modelRootDir)) {
			return true, false, fmt.Errorf("shared artifact symlink has no persisted child reference")
		}
		return false, false, nil
	}
	uri, err := getTargetDirPath(&spec)
	if err != nil {
		return true, false, err
	}
	download := func(parentPath string) error {
		return utils.Retry(s.downloadRetry, 100*time.Millisecond, func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return s.downloadModel(ctx, uri, parentPath, task)
		})
	}
	validate := func(parentPath string) (bool, error) {
		return s.validateHfOCIArtifact(ctx, task, uri, parentPath)
	}
	result, err := s.runHfArtifactDownload(ctx, task, input, allowDownload, validate, download)
	if err != nil {
		return true, false, err
	}
	switch result.Outcome {
	case hfArtifactTaskUseDefaultDownload:
		if isSharedHfArtifactSymlink(input.ChildModelPath) {
			return true, false, fmt.Errorf("refusing default download through an unrelated shared artifact symlink")
		}
		return false, false, nil
	case hfArtifactTaskRetry:
		return true, true, s.requeueHfArtifactTask(task, result)
	case hfArtifactTaskDone:
		if !allowDownload && task.NormalPriorityOnly {
			return true, true, nil
		}
		if s.modelConfigParser != nil {
			if err := s.safeParseAndUpdateModelConfig(input.ChildModelPath, task.BaseModel, task.ClusterBaseModel, nil); err != nil {
				s.logger.Errorf("Failed to parse shared artifact model configuration: %v", err)
			}
		}
		return true, false, nil
	default:
		return true, false, fmt.Errorf("unexpected shared artifact outcome %q", result.Outcome)
	}
}

func (s *Gopher) sharedHfArtifactHandler() *hfArtifactTaskHandler {
	s.hfArtifactHandlerOnce.Do(func() {
		s.hfArtifactHandler = newHfArtifactTaskHandler(newHfArtifactRepository(s.configMapReconciler))
		s.hfArtifactHandler.updateChildStatuses = s.updateHfArtifactChildLabels
	})
	return s.hfArtifactHandler
}

// lockHfChildStatus prevents ordinary task progress from publishing Ready or
// Updating labels while repair may be replacing the shared files. The repair
// callback writes labels directly while holding the same operation lock.
func (s *Gopher) lockHfChildStatus(ctx context.Context, op *NodeLabelOp) (func(), error) {
	noop := func() {}
	if s.configMapReconciler == nil || op.ModelStateOnNode == Deleted {
		return noop, nil
	}
	task := &GopherTask{TaskType: Download, BaseModel: op.BaseModel, ClusterBaseModel: op.ClusterBaseModel}
	spec := taskModelSpec(task)
	_, eligible, _ := newHfArtifactTaskInputForOCI(task, spec.Storage, s.modelRootDir)
	sharedLink := spec.Storage != nil && spec.Storage.Path != nil && isSharedHfArtifactSymlink(*spec.Storage.Path)
	if !eligible && !sharedLink {
		return noop, nil
	}
	handler := s.sharedHfArtifactHandler()
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	parent, found, err := handler.repository.GetParentForChild(ctx, key)
	if apierrors.IsNotFound(err) {
		return noop, nil
	}
	if err != nil || !found {
		return noop, err
	}
	unlock, acquired := handler.tryParentOperation(parent.Key)
	if !acquired {
		return noop, fmt.Errorf("shared artifact %s has an active operation", parent.Key)
	}
	lockedParentKey := parent.Key
	parent, found, err = handler.repository.GetParentForChild(ctx, key)
	if err == nil && (!found || parent.Key != lockedParentKey) {
		err = fmt.Errorf("shared artifact reference changed before child status update")
	}
	if err == nil && found && (op.ModelStateOnNode == Ready || len(parent.ChildStatusesBeforeRepair) != 0) &&
		(parent.Status != HfArtifactStatusReady || !handler.files.ParentReadyMarkerExists(parent)) {
		err = fmt.Errorf("shared artifact %s is not ready for child status %s", parent.Key, op.ModelStateOnNode)
	}
	if err != nil {
		unlock()
		return noop, err
	}
	return unlock, nil
}

// The repository updates ConfigMap statuses atomically with parent state. This
// callback mirrors only labels, so it cannot overwrite the saved repair states.
func (s *Gopher) updateHfArtifactChildLabels(ctx context.Context, statuses map[string]ModelStatus) error {
	if s.nodeLabelReconciler == nil {
		return nil
	}
	for key, status := range statuses {
		if err := ctx.Err(); err != nil {
			return err
		}
		namespace, name, cluster, valid := constants.ParseModelInfoFromConfigMapKey(key)
		if !valid {
			return fmt.Errorf("invalid shared artifact child key %q", key)
		}
		op := &NodeLabelOp{ModelStateOnNode: ModelStateOnNode(status)}
		if cluster {
			if s.clusterBaseModelLister == nil {
				return fmt.Errorf("ClusterBaseModel lister is unavailable for %s", key)
			}
			model, err := s.clusterBaseModelLister.Get(name)
			if apierrors.IsNotFound(err) || (err == nil && model.DeletionTimestamp != nil) {
				continue
			}
			if err != nil {
				return err
			}
			op.ClusterBaseModel = model
		} else {
			if s.baseModelLister == nil {
				return fmt.Errorf("BaseModel lister is unavailable for %s", key)
			}
			model, err := s.baseModelLister.BaseModels(namespace).Get(name)
			if apierrors.IsNotFound(err) || (err == nil && model.DeletionTimestamp != nil) {
				continue
			}
			if err != nil {
				return err
			}
			op.BaseModel = model
		}
		if err := s.nodeLabelReconciler.ReconcileNodeLabels(op); err != nil {
			return err
		}
	}
	return nil
}

// runHfArtifactDownload keeps expensive validation and writes on normal workers.
// A Failed parent with children is repaired in place, never reset as a new copy.
func (s *Gopher) runHfArtifactDownload(ctx context.Context, task *GopherTask, input hfArtifactTaskInput, allowDownload bool, validate hfArtifactValidateFunc, download hfArtifactDownloadFunc) (hfArtifactTaskResult, error) {
	handler := s.sharedHfArtifactHandler()
	parent, found, err := handler.repository.Get(ctx, input.Parent.Identity)
	if err != nil {
		return newHfArtifactRetryResult(input.Parent.Key, err), nil
	}
	if found {
		input.Parent = parent
		if !hfArtifactInputPathWithin(input.ModelStoreRoot, parent.LocalPath) {
			// One identity has one recorded parent. Do not attach
			// a child whose scan root cannot protect that parent's other links.
			return hfArtifactTaskResult{Outcome: hfArtifactTaskUseDefaultDownload}, nil
		}
	}
	if handler.childPathConflictsWithParent(input.ChildModelPath, input.Parent.LocalPath) {
		return hfArtifactTaskResult{Outcome: hfArtifactTaskUseDefaultDownload}, nil
	}
	needsRepair := task.TaskType == DownloadOverride || (found && parent.Status != HfArtifactStatusUpdating &&
		(parent.Status == HfArtifactStatusFailed || !handler.files.ParentReadyMarkerExists(parent)))
	if !allowDownload && (needsRepair || !found) {
		s.demoteToNormalPriority(task)
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
	}
	if needsRepair {
		return handler.handleDownloadOverride(ctx, input, validate, download)
	}
	return handler.handleDownload(ctx, input, download)
}

func (s *Gopher) hfArtifactInputForChild(task *GopherTask, parent HfArtifactEntry) hfArtifactTaskInput {
	key := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
	// Derive the scan boundary from the recorded parent, including custom
	// BaseModel roots outside the agent's default model directory.
	root := parent.LocalPath
	for i := 0; i < len(strings.Split(parent.Identity.ModelID, "/"))+2; i++ {
		root = filepath.Dir(root)
	}
	if s.modelRootDir != "" && hfArtifactInputPathWithin(s.modelRootDir, parent.LocalPath) &&
		hfArtifactInputPathWithin(s.modelRootDir, parent.Children[key]) {
		root = s.modelRootDir
	}
	return hfArtifactTaskInput{Parent: parent, ChildModelKey: key, ChildModelUID: types.UID(getModelUID(task)),
		ChildModelPath: parent.Children[key], ModelStoreRoot: root}
}

// detachHfArtifactForDefaultDownload releases persisted shared ownership before
// another source writes ordinary files at the child's path.
func (s *Gopher) detachHfArtifactForDefaultDownload(ctx context.Context, task *GopherTask, spec v1beta1.BaseModelSpec, allowDownload bool) (bool, error) {
	if s.configMapReconciler == nil {
		return false, nil
	}
	handler := s.sharedHfArtifactHandler()
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	parent, found, err := handler.repository.GetParentForChild(ctx, key)
	if err != nil && !apierrors.IsNotFound(err) && task.SharedArtifact {
		return true, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(key, err))
	}
	if apierrors.IsNotFound(err) {
		err = nil
	}
	if !found {
		if isSharedHfArtifactSymlink(getDestPath(&spec, s.modelRootDir)) {
			return false, fmt.Errorf("cannot replace shared child path without its persisted reference: %v", err)
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !allowDownload {
		s.demoteToNormalPriority(task)
		return true, nil
	}
	input := s.hfArtifactInputForChild(task, parent)
	preserve, err := s.isPathReferencedByOtherModels(input.ChildModelPath, task.BaseModel, task.ClusterBaseModel)
	if err != nil {
		return false, err
	}
	if preserve && filepath.Clean(input.ChildModelPath) == filepath.Clean(getDestPath(&spec, s.modelRootDir)) {
		return false, fmt.Errorf("cannot replace child path %s still used by another model", input.ChildModelPath)
	}
	result, err := s.releaseHfArtifactChild(ctx, input, preserve)
	if err == nil && result.Outcome == hfArtifactTaskRetry {
		return true, s.requeueHfArtifactTask(task, result)
	}
	return false, err
}

// processSharedHfArtifactDelete uses persisted ownership even if the CR's
// annotations, policy, source, or current local path have since changed.
func (s *Gopher) processSharedHfArtifactDelete(ctx context.Context, task *GopherTask) (handled, waiting bool, err error) {
	if s.isOrdinaryArtifactTask(task) {
		return false, false, nil
	}
	if s.configMapReconciler == nil {
		return false, false, nil
	}
	handler := s.sharedHfArtifactHandler()
	key := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
	parent, found, err := handler.repository.GetParentForChild(ctx, key)
	if apierrors.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil || !found {
		return err != nil, false, err
	}
	input := s.hfArtifactInputForChild(task, parent)
	preserve, err := s.isPathReferencedByOtherModels(input.ChildModelPath, task.BaseModel, task.ClusterBaseModel)
	if err != nil {
		return true, false, err
	}
	preserve = preserve || s.isReservingModelArtifact(task)
	result, err := s.releaseHfArtifactChild(ctx, input, preserve)
	if err == nil && result.Outcome == hfArtifactTaskRetry {
		return true, true, s.requeueHfArtifactTask(task, result)
	}
	return true, false, err
}

func (s *Gopher) releaseHfArtifactChild(ctx context.Context, input hfArtifactTaskInput, preserve bool) (hfArtifactTaskResult, error) {
	handler := s.sharedHfArtifactHandler()
	if preserve {
		unlock, acquired := handler.tryParentOperation(input.Parent.Key)
		if !acquired {
			return newHfArtifactRetryResult(input.Parent.Key, nil), nil
		}
		defer unlock()
		current, found, err := handler.repository.GetParentForChild(ctx, input.ChildModelKey)
		if err != nil {
			return newHfArtifactRetryResult(input.Parent.Key, err), nil
		}
		if !found {
			return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, nil
		}
		if current.Key != input.Parent.Key {
			return newHfArtifactRetryResult(current.Key, fmt.Errorf("child parent changed before preserving artifact")), nil
		}
		input.Parent = current
		removed, err := handler.repository.RemoveModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath)
		if err != nil {
			return newHfArtifactRetryResult(input.Parent.Key, err), nil
		}
		if removed.LastReferenceRemoved {
			if input.Parent.Status == HfArtifactStatusReady && handler.files.ParentReadyMarkerExists(input.Parent) {
				err = handler.repository.MarkReady(ctx, removed.Artifact)
			} else {
				err = handler.repository.MarkFailed(ctx, removed.Artifact)
			}
		}
		return hfArtifactTaskResult{Outcome: hfArtifactTaskDone}, err
	}
	return handler.handleDelete(ctx, input)
}

// Shared-parent waits must never fall back to writing the same directory while
// its current owner is active. The existing queue timer supplies a bounded wait.
func (s *Gopher) requeueHfArtifactTask(task *GopherTask, result hfArtifactTaskResult) error {
	if result.RetryReason != nil {
		s.logger.Warnf("Shared artifact %s needs retry: %v", result.RetryParentKey, result.RetryReason)
	}
	if s.requeueSamePathInFlightReuseWait(task, result.RetryParentKey) {
		return nil
	}
	return fmt.Errorf("shared artifact %s retry budget exhausted (last error: %v)", result.RetryParentKey, result.RetryReason)
}

func (s *Gopher) finishDownloadStatus(task *GopherTask, op *NodeLabelOp) error {
	err := s.safeNodeLabelReconciliation(op)
	if err == nil {
		return nil
	}
	s.logger.Errorf("Failed to mark model %s as Ready: %v", getModelInfoForLogging(task), err)
	spec := taskModelSpec(task)
	if isSharedHfArtifactSymlink(getDestPath(&spec, s.modelRootDir)) {
		// A sibling may acquire the parent after this child attaches. Keep the
		// successful task queued until its final Ready update is safe.
		return s.requeueHfArtifactTask(task, newHfArtifactRetryResult(gopherTaskModelKey(task), err))
	}
	return err
}

func isSharedHfArtifactSymlink(childPath string) bool {
	target, err := readChildSymlinkTarget(childPath)
	return err == nil && strings.Contains(filepath.ToSlash(target), "/_artifacts/")
}

// validateHfOCIArtifact checks existing bytes without downloading. An inspection
// error is not evidence of corruption and must not trigger destructive repair.
func (s *Gopher) validateHfOCIArtifact(ctx context.Context, task *GopherTask, uri *ociobjectstore.ObjectURI, parentPath string) (bool, error) {
	spec := taskModelSpec(task)
	store, err := s.createOCIOSDataStore(spec)
	if err != nil {
		return false, err
	}
	objects, err := store.ListObjects(*uri)
	if err != nil {
		return false, err
	}
	objects = filterInternalArtifactObjectSummaries(objects)
	if len(objects) == 0 {
		return false, fmt.Errorf("no model objects under %s", uri.Prefix)
	}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		objectURI := *uri
		objectURI.ObjectName = *object.Name
		localPath := filepath.Join(parentPath, strings.TrimPrefix(*object.Name, uri.Prefix))
		valid, err := store.IsLocalCopyValid(objectURI, localPath)
		if err != nil || !valid {
			return false, err
		}
	}
	return true, nil
}

func taskModelSpec(task *GopherTask) v1beta1.BaseModelSpec {
	if task.BaseModel != nil {
		return task.BaseModel.Spec
	}
	return task.ClusterBaseModel.Spec
}
