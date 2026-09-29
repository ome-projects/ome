package modelagent

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// Called only after attachment, while both Shared file locks remain held.
func reportSharedRestore(ctx context.Context, input hfArtifactTaskInput, parent HfArtifactEntry) error {
	return reportArtifactRestore(ctx, input.ChildModelKey, input.ChildModelUID, func(cm *corev1.ConfigMap, entry ModelEntry) error {
		stored, found, err := parentForChildEntry(cm.Data, input.ChildModelKey, entry)
		if err != nil {
			return err
		}
		if !found || stored.Key != parent.Key || stored.LocalPath != parent.LocalPath || stored.Status != HfArtifactStatusReady || stored.Children[input.ChildModelKey] != input.ChildModelPath {
			return fmt.Errorf("Shared restoration attachment changed before reporting")
		}
		return nil
	})
}
