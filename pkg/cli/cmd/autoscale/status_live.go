package autoscale

import (
	"context"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/transport"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

// statusScaleReader constructs the bounded /scale REST seam only after an
// exact IR GET. Unsupported or missing targets cause no extra read.
type statusScaleReader struct {
	factory factory.Factory
	ome     omeclient.OmeV1beta1Interface
	client  *transport.Client
}

func (r *statusScaleReader) GetInferenceReplica(ctx context.Context, namespace, name string, options metav1.GetOptions) (*ome.InferenceReplica, error) {
	request, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return r.ome.InferenceReplicas(namespace).Get(request, name, options)
}

func (r *statusScaleReader) GetInferenceReplicaScale(ctx context.Context, namespace, name string, options metav1.GetOptions) (*autoscalingv1.Scale, error) {
	if r.client == nil {
		config, err := r.factory.RESTConfig()
		if err != nil {
			return nil, err
		}
		r.client, err = transport.NewBounded(config, 1<<20)
		if err != nil {
			return nil, err
		}
	}
	request, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return r.client.GetInferenceReplicaScale(request, namespace, name, options)
}
