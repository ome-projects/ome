package snapshot

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// GPU resource-name matching. Alfred owns this set: capacity is computed from
// node allocatable and pod requests directly, with no dependency on
// AcceleratorClass status accounting.
var (
	exactGPUResources = map[corev1.ResourceName]struct{}{
		corev1.ResourceName(constants.NvidiaGPUResourceType): {}, // nvidia.com/gpu
		"amd.com/gpu": {},
	}
	prefixGPUResources = []string{
		"nvidia.com/mig-", // MIG partitions (nvidia.com/mig-1g.5gb, ...)
		"gpu.intel.com/",  // Intel GPU resources (excluding memory below)
	}
	// Intel exposes memory as a resource under the same prefix; it is not
	// a schedulable accelerator count.
	excludedGPUResourceSubstrings = []string{"memory"}
)

// IsGPUResource reports whether a resource name counts as a GPU accelerator.
func IsGPUResource(name corev1.ResourceName) bool {
	if _, ok := exactGPUResources[name]; ok {
		return true
	}
	s := string(name)
	for _, prefix := range prefixGPUResources {
		if strings.HasPrefix(s, prefix) {
			for _, excluded := range excludedGPUResourceSubstrings {
				if strings.Contains(s, excluded) {
					return false
				}
			}
			return true
		}
	}
	return false
}

// IsTPUResource reports whether a resource name counts as TPU chips. TPU pods
// occupy their node for evacuation but stay outside GPU capacity, pools and
// scoring.
func IsTPUResource(name corev1.ResourceName) bool {
	return name == corev1.ResourceName(constants.GoogleTPUResourceType)
}

// NodeGPUAllocatable returns the node's GPU resource name and allocatable
// count. Allocatable is used deliberately (not capacity): it is what the
// scheduler can actually place against. Nodes exposing several GPU resource
// names (rare; e.g. mixed MIG profiles) report the largest pool.
func NodeGPUAllocatable(node *corev1.Node) (string, int64) {
	var bestName string
	var bestCount int64
	for name, quantity := range node.Status.Allocatable {
		if !IsGPUResource(name) {
			continue
		}
		if count := quantity.Value(); count > bestCount {
			bestName, bestCount = string(name), count
		}
	}
	return bestName, bestCount
}

// PodGPURequest returns the pod's total GPU request across its regular
// containers plus native sidecars (init containers with restartPolicy
// Always), which run for the pod's whole lifetime and hold their resources
// at steady state. Ordinary init containers are ignored: they release
// resources before serving starts.
func PodGPURequest(pod *corev1.Pod) int64 {
	return podResourceRequest(pod, IsGPUResource)
}

// PodTPURequest returns the pod's TPU chip request, counted like
// PodGPURequest.
func PodTPURequest(pod *corev1.Pod) int64 {
	return podResourceRequest(pod, IsTPUResource)
}

func podResourceRequest(pod *corev1.Pod, matches func(corev1.ResourceName) bool) int64 {
	var total int64
	for i := range pod.Spec.Containers {
		total += containerResourceRequest(&pod.Spec.Containers[i], matches)
	}
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			total += containerResourceRequest(c, matches)
		}
	}
	return total
}

// containerResourceRequest evaluates each matching resource independently:
// its limit when present, else its request. Extended resources normally carry
// equal limits and requests, but an unrelated (e.g. CPU-only) limits section
// must never suppress an accelerator request, and a resource must never be
// double-counted when it appears in both maps.
func containerResourceRequest(c *corev1.Container, matches func(corev1.ResourceName) bool) int64 {
	var total int64
	for name, quantity := range c.Resources.Limits {
		if matches(name) {
			total += quantity.Value()
		}
	}
	for name, quantity := range c.Resources.Requests {
		if !matches(name) {
			continue
		}
		if _, limited := c.Resources.Limits[name]; limited {
			continue
		}
		total += quantity.Value()
	}
	return total
}

// podOccupiesNode reports whether the pod currently occupies its node: it is
// bound and not in a terminal phase. Terminating (deletion-timestamped) pods
// still hold their accelerators until they exit.
func podOccupiesNode(pod *corev1.Pod) bool {
	if pod.Spec.NodeName == "" {
		return false
	}
	return pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

// podIsReady reports the pod's Ready condition.
func podIsReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
