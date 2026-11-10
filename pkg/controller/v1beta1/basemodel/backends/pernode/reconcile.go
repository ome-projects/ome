package pernode

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// ReconcileStatusFromConfigMaps preserves the independent metadata filling and
// status updates. PVC and sharded backends bypass this per-node path.
func ReconcileStatusFromConfigMaps(ctx context.Context, c client.Client, nodeReader client.Reader, log logr.Logger, obj client.Object, isClusterScoped bool, kind string) error {
	reports, err := readAndCleanupModelReports(ctx, c, nodeReader, log, obj, isClusterScoped)
	if err != nil {
		return err
	}
	for _, report := range reports {
		config := report.entry.Config
		if config == nil {
			continue
		}
		err := retrySpecUpdate(ctx, c, log, obj, config, func(ctx context.Context, c client.Client, current client.Object, config *shared.ModelConfig) error {
			if current.GetUID() != obj.GetUID() {
				return fmt.Errorf("%s identity changed during reconciliation", kind)
			}
			spec, _, err := shared.ModelSpecAndStatus(current)
			if err != nil {
				return err
			}
			return updateModelSpecWithConfig(ctx, c, log, current, spec, config, kind)
		})
		if err != nil {
			log.Error(err, "Could not fill Model configuration", "kind", kind)
		}
	}
	firstAttempt := true
	return shared.RetryUpdate(ctx, c, log, obj, "status", func(ctx context.Context, c client.Client, current client.Object) error {
		if current.GetUID() != obj.GetUID() {
			return fmt.Errorf("%s identity changed during reconciliation", kind)
		}
		// Share the initial read with metadata filling. A conflict invalidates
		// those reports; retry against the freshly fetched Model and reports.
		if !firstAttempt {
			reports, err = readAndCleanupModelReports(ctx, c, nodeReader, log, current, isClusterScoped)
			if err != nil {
				return err
			}
		}
		firstAttempt = false
		snapshot, err := collectModelStatus(ctx, nodeReader, current, reports)
		if err != nil {
			return err
		}
		_, status, err := shared.ModelSpecAndStatus(current)
		if err != nil {
			return err
		}
		oldStatus := status.DeepCopy()
		status.NodesReady, status.NodesFailed = snapshot.ready, snapshot.failed
		status.State = snapshot.lifecycleState()
		if snapshot.completedRequestID != "" {
			status.Rehydration = &v1beta1.ModelRehydrationStatus{CompletedRequestID: snapshot.completedRequestID}
		}
		if reflect.DeepEqual(oldStatus, status) {
			return nil
		}
		shared.StampObservedReconcile(current, status)
		return c.Status().Update(ctx, current)
	})
}
