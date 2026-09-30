package capacity

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ApplyRuntimeClass adds admission overhead and required node selection.
// A conflicting authored value cannot be measured as an admitted pod.
func ApplyRuntimeClass(ctx context.Context, reader client.Reader, pod *corev1.PodSpec) (*nodev1.RuntimeClass, error) {
	if pod == nil {
		return nil, fmt.Errorf("runtime class application requires a pod")
	}
	if pod.RuntimeClassName == nil {
		if len(pod.Overhead) != 0 {
			return nil, fmt.Errorf("pod overhead has no identified RuntimeClass")
		}
		return nil, nil
	}
	if *pod.RuntimeClassName == "" {
		return nil, fmt.Errorf("RuntimeClass name is empty")
	}
	if reader == nil {
		return nil, fmt.Errorf("runtime class application requires a direct reader")
	}
	class := &nodev1.RuntimeClass{}
	if err := reader.Get(ctx, client.ObjectKey{Name: *pod.RuntimeClassName}, class); err != nil {
		return nil, err
	}
	if class.UID == "" || class.ResourceVersion == "" || !class.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("RuntimeClass %q has no verified live identity", class.Name)
	}
	var overhead corev1.ResourceList
	if class.Overhead != nil {
		overhead = class.Overhead.PodFixed
	}
	if len(pod.Overhead) != 0 && !apiequality.Semantic.DeepEqual(pod.Overhead, overhead) {
		return nil, fmt.Errorf("pod overhead differs from RuntimeClass %q", class.Name)
	}
	pod.Overhead = overhead.DeepCopy()
	if class.Scheduling != nil {
		for key, value := range class.Scheduling.NodeSelector {
			if existing, ok := pod.NodeSelector[key]; ok && existing != value {
				return nil, fmt.Errorf("pod node selector conflicts with RuntimeClass %q", class.Name)
			}
			if pod.NodeSelector == nil {
				pod.NodeSelector = map[string]string{}
			}
			pod.NodeSelector[key] = value
		}
		for _, toleration := range class.Scheduling.Tolerations {
			if !slices.ContainsFunc(pod.Tolerations, func(existing corev1.Toleration) bool { return apiequality.Semantic.DeepEqual(existing, toleration) }) {
				pod.Tolerations = append(pod.Tolerations, *toleration.DeepCopy())
			}
		}
	}
	return class, nil
}
