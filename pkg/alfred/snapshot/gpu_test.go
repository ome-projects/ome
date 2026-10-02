package snapshot

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsGPUResource(t *testing.T) {
	cases := []struct {
		name corev1.ResourceName
		want bool
	}{
		{"nvidia.com/gpu", true},
		{"amd.com/gpu", true},
		{"nvidia.com/mig-1g.5gb", true},
		{"nvidia.com/mig-3g.20gb", true},
		{"gpu.intel.com/i915", true},
		{"gpu.intel.com/i915_monitoring_memory", false}, // memory, not accelerators
		{"cpu", false},
		{"memory", false},
		{"nvidia.com/gpu-memory", false}, // not the exact resource, no matching prefix
		{"example.com/gpu", false},
	}
	for _, tc := range cases {
		if got := IsGPUResource(tc.name); got != tc.want {
			t.Errorf("IsGPUResource(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestNodeGPUAllocatable(t *testing.T) {
	node := &corev1.Node{
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("64"),
				corev1.ResourceMemory: resource.MustParse("512Gi"),
				"nvidia.com/gpu":      resource.MustParse("8"),
			},
		},
	}
	name, count := NodeGPUAllocatable(node)
	if name != "nvidia.com/gpu" || count != 8 {
		t.Fatalf("got (%q, %d), want (nvidia.com/gpu, 8)", name, count)
	}

	gpuless := &corev1.Node{Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("8"),
	}}}
	if name, count := NodeGPUAllocatable(gpuless); name != "" || count != 0 {
		t.Fatalf("gpuless node: got (%q, %d), want empty", name, count)
	}

	// Several GPU resource names on one node: the largest pool wins.
	mixed := &corev1.Node{Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		"nvidia.com/gpu":        resource.MustParse("2"),
		"nvidia.com/mig-1g.5gb": resource.MustParse("6"),
	}}}
	if name, count := NodeGPUAllocatable(mixed); name != "nvidia.com/mig-1g.5gb" || count != 6 {
		t.Fatalf("mixed node: got (%q, %d), want (nvidia.com/mig-1g.5gb, 6)", name, count)
	}
}

func podWith(limits, requests corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:      "main",
			Resources: corev1.ResourceRequirements{Limits: limits, Requests: requests},
		}}},
	}
}

func TestPodGPURequest(t *testing.T) {
	// Limits are the canonical place for extended resources.
	viaLimits := podWith(corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}, nil)
	if got := PodGPURequest(viaLimits); got != 2 {
		t.Fatalf("limits path: got %d, want 2", got)
	}

	// Requests fallback when the container sets no limits at all.
	viaRequests := podWith(nil, corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")})
	if got := PodGPURequest(viaRequests); got != 1 {
		t.Fatalf("requests fallback: got %d, want 1", got)
	}

	// A CPU-only limits section must not suppress a GPU request: each
	// resource is evaluated independently.
	cpuLimitOnly := podWith(
		corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")},
	)
	if got := PodGPURequest(cpuLimitOnly); got != 4 {
		t.Fatalf("cpu-limit-only path: got %d, want 4", got)
	}

	// A GPU resource in both maps is counted once, via its limit.
	both := podWith(
		corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")},
		corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")},
	)
	if got := PodGPURequest(both); got != 2 {
		t.Fatalf("limit+request path: got %d, want 2 (no double count)", got)
	}

	// Multi-container sums.
	multi := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}}},
		{Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/mig-1g.5gb": resource.MustParse("1")}}},
	}}}
	if got := PodGPURequest(multi); got != 3 {
		t.Fatalf("multi-container: got %d, want 3", got)
	}

	if got := PodGPURequest(podWith(nil, nil)); got != 0 {
		t.Fatalf("no resources: got %d, want 0", got)
	}
}

func TestPodGPURequestInitContainers(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	gpuOne := corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}

	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Resources: gpuOne}},
		InitContainers: []corev1.Container{
			// Native sidecar: holds its GPU for the pod's lifetime.
			{Name: "sidecar", RestartPolicy: &always, Resources: gpuOne},
			// Ordinary init container: releases resources before serving.
			{Name: "init", Resources: gpuOne},
		},
	}}
	if got := PodGPURequest(pod); got != 2 {
		t.Fatalf("got %d, want 2 (main + native sidecar; plain init excluded)", got)
	}
}
