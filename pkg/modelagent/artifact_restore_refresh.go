package modelagent

import (
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Carries the latest CR and Node already read during validation. The same
// request, live identity, and Node eligibility passed; no execution proof is kept.
type artifactRestoreSnapshotChanged struct {
	current *GopherTask
	node    *corev1.Node
}

func (*artifactRestoreSnapshotChanged) Error() string { return "artifact restoration metadata changed" }

func (s *Gopher) refreshArtifactRestoreTask(task *GopherTask, err error) error {
	var changed *artifactRestoreSnapshotChanged
	if !errors.As(err, &changed) || changed.current == nil {
		return err
	}
	// Label callbacks can validate other children. Refresh only this attempt's
	// own model and request, never adopt a sibling's snapshot as its work.
	if getModelUID(changed.current) != getModelUID(task) ||
		getModelID(changed.current.BaseModel, changed.current.ClusterBaseModel) != getModelID(task.BaseModel, task.ClusterBaseModel) ||
		artifactRehydrationID(task) == "" || artifactRehydrationID(changed.current) != artifactRehydrationID(task) {
		return err
	}
	refreshed := *task
	refreshed.BaseModel, refreshed.ClusterBaseModel = changed.current.BaseModel, changed.current.ClusterBaseModel
	// Reuse Scout's current-work classification without another API read. An
	// ignored placement or metadata update still needs this interrupted restore,
	// while source, path, policy, or repair changes retain their override semantics.
	classified := make(chan *GopherTask, 1)
	scout := &Scout{nodeInfo: changed.node, logger: s.logger, gopherChan: classified}
	if task.TensorRTLLMShapeFilter != nil {
		scout.nodeShapeAlias = task.TensorRTLLMShapeFilter.ShapeAlias
	}
	if task.BaseModel != nil {
		scout.updateBaseModel(task.BaseModel, changed.current.BaseModel)
	} else {
		scout.updateClusterBaseModel(task.ClusterBaseModel, changed.current.ClusterBaseModel)
	}
	select {
	case currentWork := <-classified:
		refreshed.TaskType = currentWork.TaskType
		refreshed.TensorRTLLMShapeFilter = currentWork.TensorRTLLMShapeFilter
		refreshed.HfResolvedRevision = ""
	default:

	}
	// Preserve the request's sequence, but start a new
	// execution with no old guard or validation result. Do not extend its budget
	// each time metadata changes or the coordinator makes it wait.
	if refreshed.restoreRefreshStartedAt.IsZero() {
		refreshed.restoreRefreshStartedAt = time.Now()
	}
	timeout := s.samePathWaitTimeout
	if timeout <= 0 {
		timeout = defaultSamePathWaitTimeout
	}
	if time.Since(refreshed.restoreRefreshStartedAt) >= timeout {
		return fmt.Errorf("artifact restoration metadata refresh timed out: %w", err)
	}
	if !s.scheduleArtifactWait(&refreshed, gopherTaskModelKey(task)) {
		return fmt.Errorf("cannot requeue artifact restoration after metadata change: %w", err)
	}
	return nil
}
