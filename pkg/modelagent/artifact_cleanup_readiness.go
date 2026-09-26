package modelagent

import (
	"context"
	"fmt"
)

func (s *Gopher) validateArtifactRepair(ctx context.Context, task *GopherTask, path string) error {
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		return err
	}
	used, err := s.sharedEvictionPathReferenced(ctx, task, path, false)
	if err != nil {
		return err
	}
	if used {
		return fmt.Errorf("cannot repair bytes referenced by another local Model")
	}
	// Initial Updating publication may have failed. Destructive repair must
	// verify withdrawal under the operation lock, just like receipt cleanup.
	return s.withdrawRestorationReadiness(ctx, task)
}

// A receipt response may have been lost before eviction withdrew Ready. Keep
// that cleanup invariant under the same file-operation locks, without using
// ordinary Deleted reconciliation (which would remove the durable receipt).
func (s *Gopher) withdrawRestorationReadiness(ctx context.Context, task *GopherTask) error {
	validate := func() error { return s.validateArtifactDownload(ctx, task) }
	if err := validate(); err != nil {
		return err
	}
	if s.nodeLabelReconciler == nil || s.kubeClient == nil {
		return fmt.Errorf("restoration cleanup requires node readiness reconciliation")
	}
	op := &NodeLabelOp{ModelStateOnNode: Updating, BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, validateCurrent: validate}
	return s.nodeLabelReconciler.WithdrawReadiness(ctx, op)
}
