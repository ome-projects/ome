package pernode

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// ReconcileStatusFromConfigMaps publishes one current Model/report snapshot.
// Conflicts rerun collection against the freshly fetched Model, including its
// UID, request, residency, source and placement; no captured config or node arrays
// cross retries. PVC and sharded backends bypass this path.
func ReconcileStatusFromConfigMaps(ctx context.Context, c client.Client, nodeReader client.Reader, log logr.Logger, obj client.Object, isClusterScoped bool, kind string) error {
	capturedSpec, _, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return err
	}
	return shared.RetryUpdate(ctx, c, log, obj, "model reports", func(ctx context.Context, c client.Client, current client.Object) error {
		if current.GetUID() != obj.GetUID() {
			return fmt.Errorf("%s identity changed during reconciliation", kind)
		}
		spec, status, err := shared.ModelSpecAndStatus(current)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(capturedSpec, spec) ||
			current.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] != obj.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] ||
			current.GetAnnotations()[constants.ModelArtifactResidencyAnnotation] != obj.GetAnnotations()[constants.ModelArtifactResidencyAnnotation] {
			return fmt.Errorf("%s inputs changed during reconciliation", kind)
		}
		snapshot, err := collectModelStatus(ctx, c, nodeReader, log, current, isClusterScoped)
		if err != nil {
			return err
		}
		oldStatus := status.DeepCopy()
		beforeSpecUpdate := current.DeepCopyObject().(client.Object)
		changed := false
		for _, config := range snapshot.configs {
			changed = shared.UpdateSpecWithConfig(spec, config) || changed
		}
		if changed {
			if err := c.Update(ctx, current); err != nil {
				if apierrors.IsConflict(err) {
					return err
				}
				// Metadata filling is optional. Preserve the existing behavior of
				// reporting readiness even when that separate write is rejected.
				log.Error(err, "Could not fill Model configuration", "kind", kind)
				current = beforeSpecUpdate
				_, status, _ = shared.ModelSpecAndStatus(current)
			}
		}
		status.NodesReady, status.NodesFailed, status.NodesEvicted = snapshot.ready, snapshot.failed, snapshot.evicted
		status.State = snapshot.lifecycleState()
		request := current.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation]
		if request != "" || status.Rehydration != nil {
			completed := ""
			if status.Rehydration != nil {
				completed = status.Rehydration.CompletedRequestID
			}
			if request != "" && len(snapshot.ready) > 0 {
				completed = request
			}
			status.Rehydration = &v1beta1.ModelRehydrationStatus{CompletedRequestID: completed}
		}
		if reflect.DeepEqual(oldStatus, status) {
			return nil
		}
		shared.StampObservedReconcile(current, status)
		return c.Status().Update(ctx, current)
	})
}
