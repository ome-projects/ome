package autoscale

import (
	"context"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/transport"
)

// statusScaleReader lazily constructs a bounded REST seam for exact IR and
// /scale GETs. Unsupported or missing targets cause no extra read.
type statusScaleReader struct {
	factory factory.Factory
	client  *transport.Client
}

func (r *statusScaleReader) GetInferenceReplica(ctx context.Context, namespace, name string, options metav1.GetOptions) (*ome.InferenceReplica, error) {
	client, err := r.boundedClient()
	if err != nil {
		return nil, err
	}
	request, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return client.GetInferenceReplica(request, namespace, name, options)
}

func (r *statusScaleReader) GetInferenceReplicaScale(ctx context.Context, namespace, name string, options metav1.GetOptions) (*autoscalingv1.Scale, error) {
	client, err := r.boundedClient()
	if err != nil {
		return nil, err
	}
	request, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return client.GetInferenceReplicaScale(request, namespace, name, options)
}

func (r *statusScaleReader) boundedClient() (*transport.Client, error) {
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
	return r.client, nil
}
