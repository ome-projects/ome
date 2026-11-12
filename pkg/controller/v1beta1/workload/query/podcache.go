package query

import (
	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// PodCacheTransform is what the manager's cache applies to a pod before it
// stores it. The cache keeps what the controllers read of a pod: its
// metadata, where and under what rules it runs (node, selectors, affinity,
// tolerations, scheduling and readiness gates, grace period), each
// container's name, image and ports, and the state the kubelet reports
// (phase, conditions, and of each container its state and the state
// before it, whether it is ready and started, its restart count and its
// image). It drops managedFields and what no controller reads from the
// cache: what the pod asks of its node (each container's requests, limits
// and claims, the pod-level resources and the overhead), how each
// container is configured (environment, command and arguments, probes,
// mounts, lifecycle hooks, security settings), the pod's volumes, and of
// each container status the image and container ids, the stop signal and
// the kubelet's reports of resources, mounts and user. A reader of a
// dropped field reads the pod live.
func PodCacheTransform() toolscache.TransformFunc {
	stripManagedFields := cache.TransformStripManagedFields()
	return func(obj any) (any, error) {
		obj, err := stripManagedFields(obj)
		if err != nil {
			return nil, err
		}
		if pod, ok := obj.(*corev1.Pod); ok {
			trimCachedPod(pod)
		}
		return obj, nil
	}
}

// trimCachedPod drops from pod the parts PodCacheTransform names. It is
// idempotent: the cache may apply it to an object it already holds.
func trimCachedPod(pod *corev1.Pod) {
	pod.Spec.Volumes = nil
	pod.Spec.Overhead = nil
	pod.Spec.Resources = nil
	for i := range pod.Spec.InitContainers {
		trimCachedContainer(&pod.Spec.InitContainers[i])
	}
	for i := range pod.Spec.Containers {
		trimCachedContainer(&pod.Spec.Containers[i])
	}
	for i := range pod.Spec.EphemeralContainers {
		trimCachedEphemeralContainer(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon)
	}
	for i := range pod.Status.InitContainerStatuses {
		trimCachedContainerStatus(&pod.Status.InitContainerStatuses[i])
	}
	for i := range pod.Status.ContainerStatuses {
		trimCachedContainerStatus(&pod.Status.ContainerStatuses[i])
	}
	for i := range pod.Status.EphemeralContainerStatuses {
		trimCachedContainerStatus(&pod.Status.EphemeralContainerStatuses[i])
	}
}

func trimCachedContainer(c *corev1.Container) {
	c.Resources = corev1.ResourceRequirements{}
	c.Command, c.Args = nil, nil
	c.Env, c.EnvFrom = nil, nil
	c.VolumeMounts, c.VolumeDevices = nil, nil
	c.LivenessProbe, c.ReadinessProbe, c.StartupProbe = nil, nil, nil
	c.Lifecycle = nil
	c.SecurityContext = nil
}

func trimCachedEphemeralContainer(c *corev1.EphemeralContainerCommon) {
	c.Resources = corev1.ResourceRequirements{}
	c.Command, c.Args = nil, nil
	c.Env, c.EnvFrom = nil, nil
	c.VolumeMounts, c.VolumeDevices = nil, nil
	c.LivenessProbe, c.ReadinessProbe, c.StartupProbe = nil, nil, nil
	c.Lifecycle = nil
	c.SecurityContext = nil
}

// trimCachedContainerStatus keeps of a container status what the readers
// of a cached pod name: the name, the state and the state before it, ready,
// started, the restart count and the image. The image and container ids
// identify the running image and process to a person; no controller reads
// them, nor the stop signal, nor the kubelet's reports of resources,
// mounts and user.
func trimCachedContainerStatus(s *corev1.ContainerStatus) {
	s.ImageID = ""
	s.ContainerID = ""
	s.StopSignal = nil
	s.AllocatedResources = nil
	s.Resources = nil
	s.VolumeMounts = nil
	s.User = nil
	s.AllocatedResourcesStatus = nil
	trimCachedContainerState(&s.State)
	trimCachedContainerState(&s.LastTerminationState)
}

// trimCachedContainerState drops the container id a terminated state
// names; its exit code, signal, reason, message and times stay.
func trimCachedContainerState(state *corev1.ContainerState) {
	if state.Terminated != nil {
		state.Terminated.ContainerID = ""
	}
}
