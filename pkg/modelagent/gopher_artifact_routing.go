package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/ome/pkg/constants"
)

// Shared ownership is sticky for this process, including after CR opt-out.
// The existing startup snapshot supplies this index without a per-task API
// lookup for ordinary models. An unavailable snapshot is retried before routing.
type gopherArtifactRouting struct {
	mutex       sync.Mutex
	known       bool
	sharedState bool
	children    map[string]bool
}

func (routing *gopherArtifactRouting) observeSnapshot(data map[string]string) error {
	routing.mutex.Lock()
	defer routing.mutex.Unlock()
	// An unreadable parent may be the only ownership record for a child.
	// Keep known ownership, but allow ordinary routing only after a full scan.
	routing.known = false
	if routing.children == nil {
		routing.children = make(map[string]bool)
	}
	for key, raw := range data {
		if strings.HasPrefix(key, constants.HfArtifactConfigMapKeyPrefix) {
			routing.sharedState = true
			var parent HfArtifactEntry
			if err := json.Unmarshal([]byte(raw), &parent); err != nil {
				return fmt.Errorf("decode shared artifact ownership %s: %w", key, err)
			}
			for child := range parent.Children {
				routing.children[child] = true
			}
			continue
		}
		var child ModelEntry
		if json.Unmarshal([]byte(raw), &child) == nil && (child.HfArtifactKey != "" || child.HfArtifactPendingDeletion != nil) {
			routing.children[key] = true
		}
	}
	routing.known = true
	return nil
}

func (s *Gopher) hasSharedArtifactTasks() bool {
	s.artifactRouting.mutex.Lock()
	defer s.artifactRouting.mutex.Unlock()
	return s.artifactRouting.sharedState || len(s.artifactRouting.children) != 0
}

func (s *Gopher) cleanupDeletingModel(current, cleanup *GopherTask) error {
	if current.SharedArtifact {
		s.enqueueTask(cleanup)
		return nil
	}
	return s.processTask(cleanup)
}

func (s *Gopher) loadArtifactRouting(ctx context.Context) error {
	s.artifactRouting.mutex.Lock()
	known := s.artifactRouting.known
	s.artifactRouting.mutex.Unlock()
	if known || s.configMapReconciler == nil {
		return nil
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return s.artifactRouting.observeSnapshot(nil)
	}
	if err != nil {
		return err
	}
	return s.artifactRouting.observeSnapshot(cm.Data)
}

// Caller holds the routing mutex through task admission, so an opt-in cannot
// race past the registration of an already-running ordinary download.
func (s *Gopher) routeArtifactTaskLocked(task *GopherTask) {
	if task.BaseModel == nil && task.ClusterBaseModel == nil {
		return
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	task.SharedArtifact = task.SharedArtifact || s.artifactRouting.children[key]
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.StorageUri == nil || spec.Storage.Path == nil {
		return
	}
	probe := *task
	probe.TaskType = Download // Deletes must recognize the same source identity.
	_, eligible, err := newHfArtifactTaskInputForOCI(&probe, spec.Storage, s.modelRootDir)
	task.SharedArtifact = task.SharedArtifact || eligible || err != nil ||
		isSharedHfArtifactSymlink(getDestPath(&spec, s.modelRootDir))
	if task.SharedArtifact {
		if s.artifactRouting.children == nil {
			s.artifactRouting.children = make(map[string]bool)
		}
		s.artifactRouting.children[key] = true
	}
}

func (s *Gopher) isOrdinaryArtifactTask(task *GopherTask) bool {
	s.artifactRouting.mutex.Lock()
	defer s.artifactRouting.mutex.Unlock()
	return s.artifactRouting.known && !task.SharedArtifact
}

// beginTask keeps shared serialization at the dispatch boundary. Ordinary
// tasks retain cancellation and deletion timing without acquiring shared state.
func (s *Gopher) beginTask(task *GopherTask) (context.Context, func(bool), bool, error) {
	ctx := context.Background()
	task.Sequence = s.taskTracker.ensureSequence(task.Sequence)
	if err := s.loadArtifactRouting(ctx); err != nil {
		return ctx, func(bool) {}, false, s.requeueHfArtifactTask(task, newHfArtifactRetryResult(gopherTaskModelKey(task), err))
	}
	s.artifactRouting.mutex.Lock()
	s.routeArtifactTaskLocked(task)
	defer s.artifactRouting.mutex.Unlock()
	key := gopherTaskModelKey(task)
	if !task.SharedArtifact {
		if task.TaskType == Delete {
			s.taskTracker.cancelLegacyDownload(key)
			attempt := s.taskTracker.beginLegacyTask(key, nil)
			return ctx, func(bool) { s.taskTracker.finishLegacyTask(attempt) }, true, nil
		}
		ctx, cancel := context.WithCancel(ctx)
		attempt := s.taskTracker.beginLegacyTask(key, cancel)
		return ctx, func(bool) { s.taskTracker.finishLegacyTask(attempt); cancel() }, true, nil
	}
	if s.isTaskModelReplaced(task) {
		return ctx, func(bool) {}, false, nil
	}
	if task.TaskType == Delete {
		attempt, outcome := s.taskTracker.beginDelete(key, task.Sequence)
		if outcome != gopherTaskProceed {
			err := s.waitForActiveTask(task, outcome)
			s.taskTracker.finishDelete(attempt, outcome == gopherTaskWait && err == nil)
			return ctx, func(bool) {}, false, err
		}
		return ctx, func(waiting bool) { s.taskTracker.finishDelete(attempt, waiting) }, true, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	attempt, outcome := s.taskTracker.beginDownload(key, task.Sequence, cancel)
	if outcome != gopherTaskProceed {
		cancel()
		return ctx, func(bool) {}, false, s.waitForActiveTask(task, outcome)
	}
	return ctx, func(bool) { s.taskTracker.finishDownload(attempt); cancel() }, true, nil
}

// A live in-process owner is not an abandoned parent. Keep queued intent until
// it finishes; the bounded parent-state retry budget must not discard it.
func (s *Gopher) waitForActiveTask(task *GopherTask, outcome gopherTaskStartResult) error {
	if outcome == gopherTaskWait {
		task.SamePathWaitStartedAt = time.Time{}
		if !s.requeueSamePathInFlightReuseWait(task, gopherTaskModelKey(task)) {
			return fmt.Errorf("cannot requeue task waiting for active model operation")
		}
	}
	return nil
}
