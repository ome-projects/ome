package pernode

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

type modelStatusSnapshot struct {
	ready, failed      []string
	evicted            bool
	completedRequestID string
	updating           bool
}

// collectModelStatus summarizes reports already filtered by orphan cleanup
// and Model identity. Completion matching does not perform API reads.
func collectModelStatus(obj client.Object, reports []nodeEntry) modelStatusSnapshot {
	var snapshot modelStatusSnapshot
	request := obj.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation]
	for _, report := range reports {
		cm, entry := report.configMap, report.entry
		switch entry.Status {
		case shared.ModelStatusReady:
			snapshot.ready = addToSlice(snapshot.ready, cm.Name)
			// Ready describes file availability. Only an acknowledgement of
			// this request and Model completes the operation.
			acknowledgesRequest := request != "" && entry.ArtifactRehydrationID == request &&
				obj.GetUID() != "" && entry.ModelUID == obj.GetUID()
			if acknowledgesRequest {
				snapshot.completedRequestID = request
			}
		case shared.ModelStatusFailed:
			snapshot.failed = addToSlice(snapshot.failed, cm.Name)
		case shared.ModelStatusEvicted:
			snapshot.evicted = true
		case shared.ModelStatusUpdating:
			snapshot.updating = true
		}
	}
	slices.Sort(snapshot.ready)
	slices.Sort(snapshot.failed)
	return snapshot
}

func (s modelStatusSnapshot) lifecycleState() v1beta1.LifeCycleState {
	if len(s.ready) > 0 {
		return v1beta1.LifeCycleStateReady
	}
	if s.updating {
		return v1beta1.LifeCycleStateInTransit
	}
	if s.evicted {
		return v1beta1.LifeCycleStateEvicted
	}
	return CalculateLifecycleState(s.ready, s.failed)
}

func CalculateLifecycleState(nodesReady, nodesFailed []string) v1beta1.LifeCycleState {
	if len(nodesReady) > 0 {
		return v1beta1.LifeCycleStateReady
	}
	if len(nodesFailed) > 0 {
		return v1beta1.LifeCycleStateFailed
	}
	return v1beta1.LifeCycleStateInTransit
}

func addToSlice(s []string, item string) []string {
	if !slices.Contains(s, item) {
		return append(s, item)
	}
	return s
}

func updateModelSpecWithConfig(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, spec *v1beta1.BaseModelSpec, config *shared.ModelConfig, modelType string) error {
	if updated := shared.UpdateSpecWithConfig(spec, config); updated {
		if err := kubeClient.Update(ctx, obj); err != nil {
			return fmt.Errorf("failed to update %s spec: %w", modelType, err)
		}
		log.Info(fmt.Sprintf("Updated %s spec with configuration data", modelType),
			"name", obj.GetName(), "namespace", obj.GetNamespace())
	}
	return nil
}

func retrySpecUpdate(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, config *shared.ModelConfig, updateFunc func(context.Context, client.Client, client.Object, *shared.ModelConfig) error) error {
	wrappedUpdateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		return updateFunc(ctx, client, obj, config)
	}
	return shared.RetryUpdate(ctx, kubeClient, log, obj, "spec", wrappedUpdateFunc)
}
