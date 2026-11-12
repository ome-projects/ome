package query

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// cachedPodFixture is a pod with every part the transform drops and every
// part it keeps set, so the test tells the two apart.
func cachedPodFixture() *corev1.Pod {
	container := corev1.Container{
		Name:  "engine",
		Image: "registry.example.com/engine:1",
		Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 30000}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{"example.com/accelerator": resource.MustParse("4")},
			Limits:   corev1.ResourceList{"example.com/accelerator": resource.MustParse("4")},
		},
		Command:         []string{"python3"},
		Args:            []string{"-m", "engine"},
		Env:             []corev1.EnvVar{{Name: "A", Value: "1"}},
		EnvFrom:         []corev1.EnvFromSource{{Prefix: "X_"}},
		VolumeMounts:    []corev1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}},
		VolumeDevices:   []corev1.VolumeDevice{{Name: "block", DevicePath: "/dev/block"}},
		LivenessProbe:   &corev1.Probe{},
		ReadinessProbe:  &corev1.Probe{},
		StartupProbe:    &corev1.Probe{},
		Lifecycle:       &corev1.Lifecycle{},
		SecurityContext: &corev1.SecurityContext{Privileged: ptr.To(true)},
	}
	status := corev1.ContainerStatus{
		Name:  "engine",
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 137, Signal: 9, Reason: "OOMKilled", Message: "out of memory", ContainerID: "containerd://abb",
			StartedAt: metav1.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC), FinishedAt: metav1.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC),
		}},
		Ready:                    true,
		RestartCount:             2,
		Image:                    "registry.example.com/engine:1",
		ImageID:                  "registry.example.com/engine@sha256:abc",
		ContainerID:              "containerd://abc",
		Started:                  ptr.To(true),
		AllocatedResources:       corev1.ResourceList{"example.com/accelerator": resource.MustParse("4")},
		Resources:                &corev1.ResourceRequirements{},
		VolumeMounts:             []corev1.VolumeMountStatus{{Name: "dshm", MountPath: "/dev/shm"}},
		User:                     &corev1.ContainerUser{Linux: &corev1.LinuxContainerUser{}},
		AllocatedResourcesStatus: []corev1.ResourceStatus{{Name: "example.com/accelerator"}},
		StopSignal:               ptr.To(corev1.SIGTERM),
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:          "example-model-engine-0-default-0",
			Namespace:     "team-a",
			Labels:        map[string]string{LabelManagedBy: ManagedByOMENative},
			Annotations:   map[string]string{"prometheus.io/scrape": "true"},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet", Operation: metav1.ManagedFieldsOperationUpdate}},
		},
		Spec: corev1.PodSpec{
			Volumes:                       []corev1.Volume{{Name: "dshm"}},
			Overhead:                      corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")},
			Resources:                     &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
			InitContainers:                []corev1.Container{*container.DeepCopy()},
			Containers:                    []corev1.Container{container},
			EphemeralContainers:           []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Env: container.Env, Resources: container.Resources}}},
			NodeName:                      "node-1",
			NodeSelector:                  map[string]string{"kubernetes.io/arch": "arm64"},
			Affinity:                      &corev1.Affinity{},
			Tolerations:                   []corev1.Toleration{{Key: "example.com/accelerator", Operator: corev1.TolerationOpExists}},
			SchedulingGates:               []corev1.PodSchedulingGate{{Name: "example.com/gate"}},
			ReadinessGates:                []corev1.PodReadinessGate{{ConditionType: ServingConditionType}},
			TerminationGracePeriodSeconds: ptr.To(int64(30)),
			Hostname:                      "example-model-engine-0-default-0",
			Subdomain:                     "example-model-engine-headless",
		},
		Status: corev1.PodStatus{
			Phase:                      corev1.PodRunning,
			Conditions:                 []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			Reason:                     "",
			PodIP:                      "10.0.0.1",
			InitContainerStatuses:      []corev1.ContainerStatus{*status.DeepCopy()},
			ContainerStatuses:          []corev1.ContainerStatus{status},
			EphemeralContainerStatuses: []corev1.ContainerStatus{*status.DeepCopy()},
		},
	}
}

func TestPodCacheTransformKeepsWhatTheControllersReadAndDropsTheRest(t *testing.T) {
	transform := PodCacheTransform()
	out, err := transform(cachedPodFixture())
	if err != nil {
		t.Fatal(err)
	}
	pod := out.(*corev1.Pod)

	// Everything the transform keeps is the fixture's.
	want := cachedPodFixture()
	want.ManagedFields = nil
	want.Spec.Volumes = nil
	want.Spec.Overhead, want.Spec.Resources = nil, nil
	for _, c := range [][]corev1.Container{want.Spec.InitContainers, want.Spec.Containers} {
		c[0].Resources = corev1.ResourceRequirements{}
		c[0].Command, c[0].Args, c[0].Env, c[0].EnvFrom = nil, nil, nil, nil
		c[0].VolumeMounts, c[0].VolumeDevices = nil, nil
		c[0].LivenessProbe, c[0].ReadinessProbe, c[0].StartupProbe, c[0].Lifecycle, c[0].SecurityContext = nil, nil, nil, nil, nil
	}
	want.Spec.EphemeralContainers[0].Env = nil
	want.Spec.EphemeralContainers[0].Resources = corev1.ResourceRequirements{}
	for _, s := range [][]corev1.ContainerStatus{want.Status.InitContainerStatuses, want.Status.ContainerStatuses, want.Status.EphemeralContainerStatuses} {
		s[0].ImageID, s[0].ContainerID, s[0].StopSignal = "", "", nil
		s[0].AllocatedResources, s[0].Resources, s[0].VolumeMounts, s[0].User, s[0].AllocatedResourcesStatus = nil, nil, nil, nil, nil
		s[0].LastTerminationState.Terminated.ContainerID = ""
	}
	if diff := cmp.Diff(want, pod); diff != "" {
		t.Fatalf("transformed pod differs from what the cache should keep (-want +got):\n%s", diff)
	}

	// The kept parts are the ones the controllers read.
	status := pod.Status.ContainerStatuses[0]
	died := status.LastTerminationState.Terminated
	if pod.Spec.NodeName == "" || pod.Spec.Affinity == nil || len(pod.Spec.Tolerations) == 0 || len(pod.Spec.ReadinessGates) == 0 ||
		pod.Spec.Containers[0].Image == "" || len(pod.Spec.Containers[0].Ports) == 0 || len(pod.Status.Conditions) == 0 {
		t.Fatalf("the transform dropped a part the controllers read: %+v", pod)
	}
	if status.Name == "" || status.State.Running == nil || status.State.Running.StartedAt.IsZero() || !status.Ready || status.Started == nil ||
		status.RestartCount != 2 || status.Image == "" || died == nil || died.ExitCode != 137 || died.Signal != 9 || died.Reason == "" ||
		died.Message == "" || died.StartedAt.IsZero() || died.FinishedAt.IsZero() {
		t.Fatalf("the transform dropped a part of the container status the controllers read: %+v", status)
	}
}

// TestPodCacheTransformDropsTheContainerIdOfATerminatedRun: the container
// id of the run a current terminated state names goes the way of the one
// in the last state; the exit code, reason and times of that run stay.
func TestPodCacheTransformDropsTheContainerIdOfATerminatedRun(t *testing.T) {
	pod := cachedPodFixture()
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: 1, Reason: "Error", Message: "exited", ContainerID: "containerd://abc",
		FinishedAt: metav1.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC),
	}}
	out, err := PodCacheTransform()(pod)
	if err != nil {
		t.Fatal(err)
	}
	died := out.(*corev1.Pod).Status.ContainerStatuses[0].State.Terminated
	if died == nil || died.ContainerID != "" || died.ExitCode != 1 || died.Reason != "Error" || died.Message != "exited" || died.FinishedAt.IsZero() {
		t.Fatalf("the cached terminated state should keep everything but the container id, got %+v", died)
	}
}

func TestPodCacheTransformIsIdempotentAndLeavesOtherKindsAlone(t *testing.T) {
	transform := PodCacheTransform()
	once, err := transform(cachedPodFixture())
	if err != nil {
		t.Fatal(err)
	}
	twice, err := transform(once.(*corev1.Pod).DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(once, twice); diff != "" {
		t.Fatalf("a second application changed the pod (-once +twice):\n%s", diff)
	}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet"}}}, Spec: corev1.NodeSpec{Unschedulable: true}}
	out, err := transform(node)
	if err != nil {
		t.Fatal(err)
	}
	got := out.(*corev1.Node)
	if got.ManagedFields != nil || !got.Spec.Unschedulable {
		t.Fatalf("a Node should lose only its managedFields, got %+v", got)
	}
}
