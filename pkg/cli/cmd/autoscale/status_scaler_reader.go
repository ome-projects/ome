package autoscale

import (
	"context"
	"encoding/json"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/transport"
)

const scalerReadTimeout = 10 * time.Second
const scalerResponseLimit = 1 << 20

var (
	hpaResource = schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}
	soResource  = schema.GroupVersionResource{Group: "keda.sh", Version: "v1alpha1", Resource: "scaledobjects"}
)

type statusScalerReader struct {
	factory  factory.Factory
	irClient *transport.Client
	dynamic  dynamic.Interface
}

func (r *statusScalerReader) GetInferenceReplica(ctx context.Context, namespace, name string, options metav1.GetOptions) (*ome.InferenceReplica, error) {
	if r.irClient == nil {
		config, err := r.factory.RESTConfig()
		if err != nil {
			return nil, err
		}
		r.irClient, err = transport.NewBounded(config, scalerResponseLimit)
		if err != nil {
			return nil, err
		}
	}
	request, cancel := context.WithTimeout(ctx, scalerReadTimeout)
	defer cancel()
	return r.irClient.GetInferenceReplica(request, namespace, name, options)
}

func (r *statusScalerReader) GetHPA(ctx context.Context, namespace, name string, options metav1.GetOptions) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	request, cancel := context.WithTimeout(ctx, scalerReadTimeout)
	defer cancel()
	raw, err := r.get(request, hpaResource, namespace, name, options)
	if err != nil {
		return nil, err
	}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := json.Unmarshal(raw, &hpa); err != nil {
		return nil, err
	}
	return &hpa, nil
}

func (r *statusScalerReader) GetScaledObject(ctx context.Context, namespace, name string, options metav1.GetOptions) (*kedav1.ScaledObject, error) {
	request, cancel := context.WithTimeout(ctx, scalerReadTimeout)
	defer cancel()
	raw, err := r.get(request, soResource, namespace, name, options)
	if err != nil {
		return nil, err
	}
	var so kedav1.ScaledObject
	if err := json.Unmarshal(raw, &so); err != nil {
		return nil, err
	}
	return &so, nil
}

func (r *statusScalerReader) get(ctx context.Context, resource schema.GroupVersionResource, namespace, name string, options metav1.GetOptions) ([]byte, error) {
	if r.dynamic == nil {
		config, err := r.factory.RESTConfig()
		if err != nil {
			return nil, err
		}
		r.dynamic, err = transport.NewBoundedDynamic(config, scalerResponseLimit)
		if err != nil {
			return nil, err
		}
	}
	object, err := r.dynamic.Resource(resource).Namespace(namespace).Get(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return json.Marshal(object.Object)
}
