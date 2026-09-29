package engine

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// dispatchCaptureReader anchors the policy builder's capacity, demand and
// ownership inputs to one accepted scheduler capture. Only model/storage
// metadata may be read live; missing captured lists must never fall back.
type dispatchCaptureReader struct {
	capture  *input.Snapshot
	metadata client.Reader
}

func (r *dispatchCaptureReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	switch object.(type) {
	case *v1beta1.BaseModel, *v1beta1.ClusterBaseModel, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume:
		return r.metadata.Get(ctx, key, object, opts...)
	default:
		return fmt.Errorf("uncaptured scoring get %T", object)
	}
}

func (r *dispatchCaptureReader) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if r.capture == nil || len(opts) != 0 {
		return fmt.Errorf("scoring requires complete captured lists")
	}
	switch typed := list.(type) {
	case *v1beta1.InferenceServiceList:
		*typed = *(&v1beta1.InferenceServiceList{Items: r.capture.InferenceServices}).DeepCopy()
		return nil
	case *v1beta1.InferenceReplicaList:
		*typed = *(&v1beta1.InferenceReplicaList{Items: r.capture.InferenceReplicas}).DeepCopy()
		return nil
	case *corev1.NodeList:
		*typed = corev1.NodeList{}
	case *corev1.PodList:
		*typed = corev1.PodList{}
	default:
		return fmt.Errorf("uncaptured scoring list %T", list)
	}
	for _, object := range r.capture.Objects {
		var header metav1.TypeMeta
		if err := json.Unmarshal(object.Raw, &header); err != nil {
			return err
		}
		if header.APIVersion != "v1" {
			continue
		}
		switch typed := list.(type) {
		case *corev1.NodeList:
			if header.Kind == "Node" {
				var node corev1.Node
				if err := json.Unmarshal(object.Raw, &node); err != nil {
					return err
				}
				typed.Items = append(typed.Items, node)
			}
		case *corev1.PodList:
			if header.Kind == "Pod" {
				var pod corev1.Pod
				if err := json.Unmarshal(object.Raw, &pod); err != nil {
					return err
				}
				typed.Items = append(typed.Items, pod)
			}
		}
	}
	return nil
}
