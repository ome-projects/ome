package pernode

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/modelagent"
)

type modelStatusSnapshot struct {
	ready, failed      []string
	evicted            bool
	completedRequestID string
	updating           bool
}

// collectModelStatus summarizes already decoded reports. Only completion
// verification reads the current Node identity; report cleanup happens before it.
func collectModelStatus(ctx context.Context, nodeReader client.Reader, obj client.Object, reports []nodeEntry) (modelStatusSnapshot, error) {
	var snapshot modelStatusSnapshot
	request := obj.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation]
	for _, report := range reports {
		cm, entry := report.configMap, report.entry
		switch entry.Status {
		case modelagent.ModelStatusReady:
			snapshot.ready = addToSlice(snapshot.ready, cm.Name)
			// Ready describes file availability. Only an acknowledgement of
			// this request, Model and Node instance completes the operation.
			acknowledgesRequest := request != "" && entry.ArtifactRehydrationID == request &&
				obj.GetUID() != "" && entry.ModelUID == obj.GetUID() && entry.NodeUID != ""
			if acknowledgesRequest && snapshot.completedRequestID == "" {
				if nodeReader == nil {
					return snapshot, fmt.Errorf("restoration completion requires a Node reader")
				}
				node := &corev1.Node{}
				if err := nodeReader.Get(ctx, client.ObjectKey{Name: cm.Name}, node); err != nil {
					if !apierrors.IsNotFound(err) {
						return snapshot, err
					}
				} else if node.UID == entry.NodeUID {
					snapshot.completedRequestID = request
				}
			}
		case modelagent.ModelStatusFailed:
			snapshot.failed = addToSlice(snapshot.failed, cm.Name)
		case modelagent.ModelStatusEvicted:
			snapshot.evicted = true
		case modelagent.ModelStatusUpdating:
			snapshot.updating = true
		}
	}
	slices.Sort(snapshot.ready)
	slices.Sort(snapshot.failed)
	return snapshot, nil
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

func updateModelSpecWithConfig(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, spec *v1beta1.BaseModelSpec, config *modelagent.ModelConfig, modelType string) error {
	if updated := shared.UpdateSpecWithConfig(spec, config); updated {
		if err := kubeClient.Update(ctx, obj); err != nil {
			return fmt.Errorf("failed to update %s spec: %w", modelType, err)
		}
		log.Info(fmt.Sprintf("Updated %s spec with configuration data", modelType),
			"name", obj.GetName(), "namespace", obj.GetNamespace())
	}
	return nil
}

func retrySpecUpdate(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, config *modelagent.ModelConfig, updateFunc func(context.Context, client.Client, client.Object, *modelagent.ModelConfig) error) error {
	wrappedUpdateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		return updateFunc(ctx, client, obj, config)
	}
	return shared.RetryUpdate(ctx, kubeClient, log, obj, "spec", wrappedUpdateFunc)
}
