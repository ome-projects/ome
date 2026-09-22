package pod

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/constants"
)

func makePod(annos map[string]string, containers ...v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: annos},
		Spec:       v1.PodSpec{Containers: containers},
	}
}

func defaultContainer() v1.Container {
	return v1.Container{Name: DefaultContainerName}
}

// =========================================================================
// SHM
// =========================================================================

func TestShmInjector_NoAnnotation_NoOp(t *testing.T) {
	pod := makePod(nil, defaultContainer())
	assert.NoError(t, NewShmInjector().InjectShm(pod))
	assert.Empty(t, pod.Spec.Volumes)
	assert.Empty(t, pod.Spec.Containers[0].VolumeMounts)
}

func TestShmInjector_DefaultProfile_AddsVolumeAndMount(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeShmProfileAnnotationKey: "default",
	}, defaultContainer())
	assert.NoError(t, NewShmInjector().InjectShm(pod))
	assert.Len(t, pod.Spec.Volumes, 1)
	assert.Equal(t, DshmVolumeName, pod.Spec.Volumes[0].Name)
	assert.Equal(t, v1.StorageMediumMemory, pod.Spec.Volumes[0].EmptyDir.Medium)
	assert.Nil(t, pod.Spec.Volumes[0].EmptyDir.SizeLimit, "default profile must not set SizeLimit")
	assert.Len(t, pod.Spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, "/dev/shm", pod.Spec.Containers[0].VolumeMounts[0].MountPath)
}

func TestShmInjector_UnknownProfile_Errors(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeShmProfileAnnotationKey: "bogus",
	}, defaultContainer())
	err := NewShmInjector().InjectShm(pod)
	assert.Error(t, err)
}

func TestShmInjector_ExistingVolumeNotDuplicated(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeShmProfileAnnotationKey: "default",
	}, defaultContainer())
	pod.Spec.Volumes = []v1.Volume{
		{Name: DshmVolumeName, VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		}},
	}
	pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
		{Name: DshmVolumeName, MountPath: "/dev/shm"},
	}
	assert.NoError(t, NewShmInjector().InjectShm(pod))
	assert.Len(t, pod.Spec.Volumes, 1, "should not duplicate dshm volume")
	assert.Len(t, pod.Spec.Containers[0].VolumeMounts, 1, "should not duplicate dshm mount")
}

func TestShmInjector_TargetContainerMissing_NoOp(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeShmProfileAnnotationKey:    "default",
		constants.RuntimeContainerNameAnnotationKey: "missing",
	}, defaultContainer())
	assert.NoError(t, NewShmInjector().InjectShm(pod))
	assert.Empty(t, pod.Spec.Volumes, "no work if target container is absent")
}

// =========================================================================
// PROBE
// =========================================================================

func TestProbeInjector_NoAnnotation_NoOp(t *testing.T) {
	pod := makePod(nil, defaultContainer())
	assert.NoError(t, NewProbeInjector().InjectProbes(pod))
	assert.Nil(t, pod.Spec.Containers[0].ReadinessProbe)
	assert.Nil(t, pod.Spec.Containers[0].LivenessProbe)
	assert.Nil(t, pod.Spec.Containers[0].StartupProbe)
}

func TestProbeInjector_SglangHttp_DefaultPort(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeProbeProfileAnnotationKey: "sglang-http",
	}, defaultContainer())
	assert.NoError(t, NewProbeInjector().InjectProbes(pod))
	c := pod.Spec.Containers[0]
	for _, probe := range []*v1.Probe{c.ReadinessProbe, c.LivenessProbe, c.StartupProbe} {
		if assert.NotNil(t, probe) {
			assert.Equal(t, "/health", probe.HTTPGet.Path)
			assert.Equal(t, intstr.FromInt32(DefaultRuntimeProbePort), probe.HTTPGet.Port)
		}
	}
}

func TestProbeInjector_PortOverride(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeProbeProfileAnnotationKey: "sglang-http",
		constants.RuntimeProbePortAnnotationKey:    "8080",
	}, defaultContainer())
	assert.NoError(t, NewProbeInjector().InjectProbes(pod))
	assert.Equal(t, intstr.FromInt32(8080), pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Port)
}

func TestProbeInjector_InvalidPort_Errors(t *testing.T) {
	for _, bad := range []string{"abc", "0", "70000", "-1"} {
		pod := makePod(map[string]string{
			constants.RuntimeProbeProfileAnnotationKey: "sglang-http",
			constants.RuntimeProbePortAnnotationKey:    bad,
		}, defaultContainer())
		assert.Error(t, NewProbeInjector().InjectProbes(pod), "port=%q should error", bad)
	}
}

func TestProbeInjector_DoesNotOverrideExistingProbe(t *testing.T) {
	existing := &v1.Probe{ProbeHandler: v1.ProbeHandler{
		HTTPGet: &v1.HTTPGetAction{Path: "/custom", Port: intstr.FromInt32(9999)},
	}}
	c := defaultContainer()
	c.ReadinessProbe = existing
	pod := makePod(map[string]string{
		constants.RuntimeProbeProfileAnnotationKey: "sglang-http",
	}, c)
	assert.NoError(t, NewProbeInjector().InjectProbes(pod))
	got := pod.Spec.Containers[0].ReadinessProbe
	assert.Equal(t, "/custom", got.HTTPGet.Path, "existing readinessProbe must not be overwritten")
	assert.NotNil(t, pod.Spec.Containers[0].LivenessProbe, "missing liveness still gets injected")
}

func TestProbeInjector_UnknownProfile_Errors(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeProbeProfileAnnotationKey: "bogus",
	}, defaultContainer())
	assert.Error(t, NewProbeInjector().InjectProbes(pod))
}

// =========================================================================
// OBSERVABILITY
// =========================================================================

func TestObservabilityInjector_NoAnnotation_NoOp(t *testing.T) {
	pod := makePod(nil, defaultContainer())
	assert.NoError(t, NewObservabilityInjector().InjectObservability(pod))
	assert.Empty(t, pod.Annotations)
}

func TestObservabilityInjector_PrometheusProfile_SetsAnnotations(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
		constants.RuntimeObservabilityPortAnnotationKey:    "8080",
	}, defaultContainer())
	assert.NoError(t, NewObservabilityInjector().InjectObservability(pod))
	assert.Equal(t, "true", pod.Annotations[constants.PrometheusScrapeAnnotationKey])
	assert.Equal(t, "8080", pod.Annotations[constants.PrometheusPortAnnotationKey])
	assert.Equal(t, "/metrics", pod.Annotations[constants.PrometheusPathAnnotationKey])
}

func TestObservabilityInjector_DefaultPort(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
	}, defaultContainer())
	assert.NoError(t, NewObservabilityInjector().InjectObservability(pod))
	assert.Equal(t, "30000", pod.Annotations[constants.PrometheusPortAnnotationKey])
}

func TestObservabilityInjector_DoesNotOverrideExisting(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
		constants.PrometheusPortAnnotationKey:              "9999", // operator-set
	}, defaultContainer())
	assert.NoError(t, NewObservabilityInjector().InjectObservability(pod))
	assert.Equal(t, "9999", pod.Annotations[constants.PrometheusPortAnnotationKey],
		"operator-set port must be preserved")
}

func TestObservabilityInjector_UnknownProfile_Errors(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RuntimeObservabilityProfileAnnotationKey: "bogus",
	}, defaultContainer())
	assert.Error(t, NewObservabilityInjector().InjectObservability(pod))
}

// =========================================================================
// RDMA cks-gb-sglang profile
// =========================================================================

func TestRDMAProfiles_cksGbSglang_HasExpectedShape(t *testing.T) {
	p, ok := RDMAProfiles["cks-gb-sglang"]
	assert.True(t, ok)
	want := []string{
		"MC_TE_METRIC",
		"NCCL_MNNVL_ENABLE",
		"NCCL_CUMEM_ENABLE",
		"NCCL_IB_ADDR_FAMILY",
		"NCCL_SOCKET_IFNAME",
		"NCCL_SOCKET_FAMILY",
	}
	for _, k := range want {
		_, has := p.EnvVars[k]
		assert.True(t, has, "missing %q", k)
	}
	assert.Len(t, p.EnvVars, len(want), "extra env vars — add them to the want list deliberately")
	// Pin the absence of env vars deliberately excluded from this
	// profile so a re-add is a deliberate decision.
	for _, k := range []string{
		"PYTHONUNBUFFERED",
		"GLOO_SOCKET_IFNAME",
		"SGLANG_SET_CPU_AFFINITY",
		"SGLANG_ENABLE_JIT_DEEPGEMM",
		"NVSHMEM_IB_GID_INDEX",
		"NVSHMEM_ENABLE_NIC_PE_MAPPING",
	} {
		_, has := p.EnvVars[k]
		assert.False(t, has, "%q is excluded on purpose", k)
	}
	assert.Len(t, p.Volumes, 2)
	assert.Len(t, p.VolumeMounts, 2)
	assert.NotNil(t, p.SecurityContext)
	// Not privileged: that would bypass the device cgroup and hand the pod
	// every GPU on the node instead of its allocated ones (#833).
	assert.Nil(t, p.SecurityContext.Privileged)
}
