package pernode

import (
	"context"
	"fmt"
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// readModelServing observes usage, not deletion permission. A future LRU must
// repeat live consumer checks at deletion time; status can always lag demand.
func readModelServing(ctx context.Context, reader client.Reader, obj client.Object, previous *v1beta1.ModelServingStatus) (*v1beta1.ModelServingStatus, error) {
	if reader == nil {
		return nil, fmt.Errorf("serving usage requires an API reader")
	}
	inUse, err := modelHasServingConsumers(ctx, reader, obj)
	if err != nil {
		return nil, err
	}
	next := &v1beta1.ModelServingStatus{InUse: inUse}
	if !inUse && previous != nil {
		if previous.InUse {
			now := metav1.Now()
			next.LastUsedTime = &now
		} else {
			next.LastUsedTime = previous.LastUsedTime.DeepCopy()
		}
	}
	return next, nil
}

func modelHasServingConsumers(ctx context.Context, reader client.Reader, obj client.Object) (bool, error) {
	services := &v1beta1.InferenceServiceList{}
	if err := reader.List(ctx, services); err != nil {
		return false, fmt.Errorf("list serving services: %w", err)
	}
	for _, service := range services.Items {
		// Existence is demand, including Pending, zero replicas and termination.
		ref := service.Spec.Model
		if ref == nil || ref.Name == "" {
			used, err := servingReferenceMatches(ctx, reader, obj, service.Namespace, service.Annotations[constants.BaseModelName], nil, nil)
			if err != nil || used {
				return used, err
			}
		}
		if ref != nil {
			kind, group := ref.Kind, ref.APIGroup
			// Match primary workload resolution: ungated services stay namespaced-first
			// even when the API defaulted their kind to ClusterBaseModel.
			if _, gated := service.Annotations[constants.ArtifactStartupGateAnnotation]; !gated {
				kind, group = nil, nil
			}
			used, err := servingReferenceMatches(ctx, reader, obj, service.Namespace, ref.Name, kind, group)
			if err != nil || used {
				return used, err
			}
			for _, overlay := range ref.Overlays {
				used, err := servingReferenceMatches(ctx, reader, obj, service.Namespace, overlay.Name, overlay.Kind, overlay.APIGroup)
				if err != nil || used {
					return used, err
				}
			}
		}
	}

	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods); err != nil {
		return false, fmt.Errorf("list serving pods: %w", err)
	}
	spec, _, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return false, err
	}
	label := constants.GetBaseModelLabel(obj.GetNamespace(), obj.GetName())
	if _, cluster := obj.(*v1beta1.ClusterBaseModel); cluster {
		label = constants.GetClusterBaseModelLabel(obj.GetName())
	}
	requestLabel, _ := constants.ArtifactReadyLabelKey(obj.GetUID())
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if _, found := pod.Spec.NodeSelector[label]; found {
			return true, nil
		}
		if _, found := pod.Spec.NodeSelector[requestLabel]; requestLabel != "" && found {
			return true, nil
		}
		if obj.GetUID() != "" && pod.Annotations[constants.ArtifactModelUIDAnnotation] == string(obj.GetUID()) {
			return true, nil
		}
		// Explicit primary identity takes precedence over legacy names, but
		// independent selectors above and mounted paths below still count.
		if pod.Annotations[constants.ArtifactModelUIDAnnotation] == "" {
			for _, name := range []string{pod.Annotations[constants.BaseModelName], pod.Labels[constants.InferenceServiceBaseModelNameLabelKey]} {
				_, namespacedSelector := pod.Spec.NodeSelector[constants.GetBaseModelLabel(pod.Namespace, name)]
				_, clusterSelector := pod.Spec.NodeSelector[constants.GetClusterBaseModelLabel(name)]
				if namespacedSelector || clusterSelector {
					continue
				}
				used, err := servingReferenceMatches(ctx, reader, obj, pod.Namespace, name, nil, nil)
				if err != nil || used {
					return used, err
				}
			}
		}
		if spec.Storage != nil && spec.Storage.Path != nil && servingPodUsesPath(&pod, *spec.Storage.Path) {
			return true, nil
		}
	}
	return false, nil
}

func servingReferenceMatches(ctx context.Context, reader client.Reader, obj client.Object, namespace, name string, kind, group *string) (bool, error) {
	if name == "" || name != obj.GetName() || (group != nil && *group != "" && *group != v1beta1.SchemeGroupVersion.Group) {
		return false, nil
	}
	_, cluster := obj.(*v1beta1.ClusterBaseModel)
	if kind != nil && *kind != "" {
		return (*kind == "ClusterBaseModel" && cluster) || (*kind == "BaseModel" && !cluster && namespace == obj.GetNamespace()), nil
	}
	if !cluster {
		return namespace == obj.GetNamespace(), nil
	}
	// Legacy unqualified references prefer a namespaced model.
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &v1beta1.BaseModel{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("resolve serving model %s/%s: %w", namespace, name, err)
	}
	return false, nil
}

func servingPodUsesPath(pod *corev1.Pod, modelPath string) bool {
	if !path.IsAbs(modelPath) {
		return false
	}
	modelPath = path.Clean(modelPath)
	within := func(candidate, root string) bool {
		candidate, root = path.Clean(candidate), path.Clean(root)
		return candidate == root || strings.HasPrefix(candidate, strings.TrimSuffix(root, "/")+"/")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.HostPath == nil {
			continue
		}
		root := volume.HostPath.Path
		if within(root, modelPath) {
			return true
		}
		// Broad agent root mounts alone do not consume every model. Subpaths
		// can identify a consumer; dynamic overlapping subpaths are uncertain.
		for _, containers := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
			for _, container := range containers {
				for _, mount := range container.VolumeMounts {
					if mount.Name != volume.Name {
						continue
					}
					if mount.SubPath != "" {
						mountedPath := path.Join(root, mount.SubPath)
						if within(mountedPath, modelPath) || within(modelPath, mountedPath) {
							return true
						}
					}
					if mount.SubPathExpr != "" && within(modelPath, root) {
						return true
					}
				}
			}
		}
	}
	return false
}
