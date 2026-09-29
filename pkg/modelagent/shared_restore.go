package modelagent

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// Called only after attachment, while both Shared file locks remain held.
func reportSharedRestore(ctx context.Context, input hfArtifactTaskInput, parent HfArtifactEntry) error {
	guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard)
	if guard == nil {
		return nil
	}
	s := guard.gopher
	// Check startup validation before the ConfigMap write lock, matching recovery's
	// lock order. File locks keep this parent stable through publication.
	if !guard.validated && s.hfArtifactStartup.needsValidation(parent.Key) {
		return fmt.Errorf("Shared restoration requires pending startup validation")
	}
	return reportArtifactRestore(ctx, input.ChildModelKey, input.ChildModelUID, func(cm *corev1.ConfigMap, entry ModelEntry) error {
		stored, found, err := parentForChildEntry(cm.Data, input.ChildModelKey, entry)
		if err != nil {
			return err
		}
		if !found || stored.Key != parent.Key || stored.LocalPath != parent.LocalPath || stored.Status != HfArtifactStatusReady || stored.Children[input.ChildModelKey] != input.ChildModelPath {
			return fmt.Errorf("Shared restoration attachment changed before reporting")
		}
		files := s.sharedHfArtifactHandler().files
		if !files.ParentReadyMarkerExists(stored) || !files.IsChildLinkedToParent(input.ChildModelPath, stored.LocalPath) {
			return fmt.Errorf("Shared restoration files are not attached to the Ready parent")
		}
		guard.validated = true
		return nil
	})
}
