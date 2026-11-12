package pod

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestRDMAInjector_InjectRDMA(t *testing.T) {
	tests := []struct {
		name           string
		pod            *v1.Pod
		expectedError  bool
		expectedResult *v1.Pod
		description    string
	}{
		{
			name: "no_auto_inject_annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
					},
				},
			},
			expectedError: false,
			expectedResult: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
					},
				},
			},
			description: "Pod without auto-inject annotation should not be modified",
		},
		{
			name: "auto_inject_with_default_profile_and_container",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
					},
				},
			},
			expectedError: false,
			expectedResult: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Env:  getExpectedEnvVars(t),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      devInfVolumeName,
									MountPath: "/dev/infiniband",
								},
							},
							SecurityContext: &v1.SecurityContext{
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"IPC_LOCK",
									},
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: DshmVolumeName,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
						{
							Name: devInfVolumeName,
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/dev/infiniband",
								},
							},
						},
					},
				},
			},
			description: "Pod with auto-inject annotation should have RDMA config injected into default container",
		},
		{
			name: "auto_inject_with_custom_container_name",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey:    "true",
						constants.RDMAContainerNameAnnotationKey: "custom-container",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
						{
							Name: "custom-container",
						},
					},
				},
			},
			expectedError: false,
			expectedResult: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey:    "true",
						constants.RDMAContainerNameAnnotationKey: "custom-container",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
						{
							Name: "custom-container",
							Env:  getExpectedEnvVars(t),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      devInfVolumeName,
									MountPath: "/dev/infiniband",
								},
							},
							SecurityContext: &v1.SecurityContext{
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"IPC_LOCK",
									},
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: DshmVolumeName,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
						{
							Name: devInfVolumeName,
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/dev/infiniband",
								},
							},
						},
					},
				},
			},
			description: "Pod with auto-inject and container-name annotations should have RDMA config injected into the specified container",
		},
		{
			name: "container_not_found",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey:    "true",
						constants.RDMAContainerNameAnnotationKey: "nonexistent-container",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
					},
				},
			},
			expectedError: false,
			expectedResult: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey:    "true",
						constants.RDMAContainerNameAnnotationKey: "nonexistent-container",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
					},
				},
			},
			description: "No error should be returned when the specified container is not found, and pod should not be modified",
		},
		{
			name: "auto_inject_with_existing_security_context",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							SecurityContext: &v1.SecurityContext{
								RunAsUser: &[]int64{1000}[0],
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"NET_ADMIN",
									},
								},
							},
						},
					},
				},
			},
			expectedError: false,
			expectedResult: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Env:  getExpectedEnvVars(t),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      devInfVolumeName,
									MountPath: "/dev/infiniband",
								},
							},
							SecurityContext: &v1.SecurityContext{
								RunAsUser: &[]int64{1000}[0],
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"NET_ADMIN",
										"IPC_LOCK",
									},
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: DshmVolumeName,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
						{
							Name: devInfVolumeName,
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/dev/infiniband",
								},
							},
						},
					},
				},
			},
			description: "Existing security context should be preserved and enhanced with RDMA capabilities",
		},
		{
			name: "auto_inject_with_existing_volumes_and_mounts",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      "existing-volume",
									MountPath: "/data",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: DshmVolumeName,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
						{
							Name: "existing-volume",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
			expectedError: false,
			expectedResult: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Env:  getExpectedEnvVars(t),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      "existing-volume",
									MountPath: "/data",
								},
								{
									Name:      devInfVolumeName,
									MountPath: "/dev/infiniband",
								},
							},
							SecurityContext: &v1.SecurityContext{
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"IPC_LOCK",
									},
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: DshmVolumeName,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
						{
							Name: "existing-volume",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: devInfVolumeName,
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/dev/infiniband",
								},
							},
						},
					},
				},
			},
			description: "Existing volumes and mounts should be preserved",
		},
		{
			name: "auto_inject_with_custom_profile",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
						constants.RDMAProfileAnnotationKey:    "roce",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
					},
				},
			},
			expectedError: false,
			expectedResult: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
						constants.RDMAProfileAnnotationKey:    "roce",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Env:  getExpectedEnvVars(t),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      devInfVolumeName,
									MountPath: "/dev/infiniband",
								},
							},
							SecurityContext: &v1.SecurityContext{
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"IPC_LOCK",
									},
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: DshmVolumeName,
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
						{
							Name: devInfVolumeName,
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/dev/infiniband",
								},
							},
						},
					},
				},
			},
			description: "Pod with custom profile annotation should use that profile",
		},
		{
			name: "invalid_profile",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						constants.RDMAAutoInjectAnnotationKey: "true",
						constants.RDMAProfileAnnotationKey:    "nonexistent-profile",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
						},
					},
				},
			},
			expectedError:  true,
			expectedResult: nil,
			description:    "Error should be returned when the specified profile does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := injectRDMA(tt.pod, fixtureRDMA(t))

			if tt.expectedError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)

				// Check structure and properties without relying on exact env var order
				assertPodMatches(t, tt.expectedResult, tt.pod, tt.description)
			}
		})
	}
}

func TestRDMAInjector_injectRDMAConfig(t *testing.T) {
	// Test with a pod that already has all the volumes and mounts
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "ome-container",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      DshmVolumeName,
							MountPath: "/dev/shm",
						},
						{
							Name:      devInfVolumeName,
							MountPath: "/dev/infiniband",
						},
					},
					SecurityContext: &v1.SecurityContext{
						Capabilities: &v1.Capabilities{
							Add: []v1.Capability{
								"IPC_LOCK",
							},
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: DshmVolumeName,
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium: v1.StorageMediumMemory,
						},
					},
				},
				{
					Name: devInfVolumeName,
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/dev/infiniband",
						},
					},
				},
			},
		},
	}

	profile := fixtureRDMA(t).Profiles["roce"]
	err := injectRDMAConfig(pod, profile)

	assert.NoError(t, err)
	// Verify that the environment variables were added
	assert.Equal(t, len(profile.EnvVars), len(pod.Spec.Containers[0].Env))
}

func TestRDMAInjector_volumeMountExists(t *testing.T) {
	container := &v1.Container{
		Name: "test-container",
		VolumeMounts: []v1.VolumeMount{
			{
				Name:      "existing-volume",
				MountPath: "/path",
			},
		},
	}

	assert.True(t, hasVolumeMount(container, "existing-volume"))
	assert.False(t, hasVolumeMount(container, "non-existing-volume"))
}

func TestRDMAInjector_volumeExists(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "existing-volume",
				},
			},
		},
	}

	assert.True(t, hasVolume(pod, "existing-volume"))
	assert.False(t, hasVolume(pod, "non-existing-volume"))
}

func TestRDMAInjector_capabilityExists(t *testing.T) {
	capabilities := []v1.Capability{"CAP1", "CAP2"}

	assert.True(t, hasCapability(capabilities, "CAP1"))
	assert.False(t, hasCapability(capabilities, "CAP3"))
}

func TestRDMAInjector_injectVolumes(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "existing-volume",
				},
			},
		},
	}

	newVolumes := []v1.Volume{
		{
			Name: "new-volume",
		},
		{
			Name: "existing-volume", // This shouldn't be added again
		},
	}

	injectVolumes(pod, newVolumes)

	assert.Len(t, pod.Spec.Volumes, 2)
	assert.Equal(t, "existing-volume", pod.Spec.Volumes[0].Name)
	assert.Equal(t, "new-volume", pod.Spec.Volumes[1].Name)
}

func TestRDMAInjector_injectContainerConfig(t *testing.T) {
	container := &v1.Container{
		Name: "test-container",
	}

	profile := RDMAProfile{
		EnvVars: map[string]string{
			"ENV1": "value1",
			"ENV2": "value2",
		},
		VolumeMounts: []v1.VolumeMount{
			{
				Name:      "volume1",
				MountPath: "/path1",
			},
		},
		SecurityContext: &v1.SecurityContext{
			Capabilities: &v1.Capabilities{
				Add: []v1.Capability{
					"CAP1",
				},
			},
			Privileged: &[]bool{true}[0],
		},
	}

	injectContainerConfig(container, profile)

	// Verify environment variables
	assert.Len(t, container.Env, 2)
	assert.Equal(t, "ENV1", container.Env[0].Name)
	assert.Equal(t, "value1", container.Env[0].Value)
	assert.Equal(t, "ENV2", container.Env[1].Name)
	assert.Equal(t, "value2", container.Env[1].Value)

	// Verify volume mounts
	assert.Len(t, container.VolumeMounts, 1)
	assert.Equal(t, "volume1", container.VolumeMounts[0].Name)
	assert.Equal(t, "/path1", container.VolumeMounts[0].MountPath)

	// Verify security context
	assert.NotNil(t, container.SecurityContext)
	assert.True(t, *container.SecurityContext.Privileged)
	assert.Len(t, container.SecurityContext.Capabilities.Add, 1)
	assert.Equal(t, v1.Capability("CAP1"), container.SecurityContext.Capabilities.Add[0])
}

// assertPodMatches compares two pods, but does not rely on the exact order of environment variables
func assertPodMatches(t *testing.T, expected, actual *v1.Pod, message string) {
	// Check general pod structure
	assert.Equal(t, expected.ObjectMeta.Name, actual.ObjectMeta.Name, "Pod name should match")
	assert.Equal(t, expected.ObjectMeta.Annotations, actual.ObjectMeta.Annotations, "Pod annotations should match")

	// Check volumes
	assert.Equal(t, len(expected.Spec.Volumes), len(actual.Spec.Volumes), "Number of volumes should match")
	for _, expectedVol := range expected.Spec.Volumes {
		found := false
		for _, actualVol := range actual.Spec.Volumes {
			if expectedVol.Name == actualVol.Name {
				assert.Equal(t, expectedVol, actualVol, "Volume %s should match", expectedVol.Name)
				found = true
				break
			}
		}
		assert.True(t, found, "Expected volume %s not found", expectedVol.Name)
	}

	// Check containers
	assert.Equal(t, len(expected.Spec.Containers), len(actual.Spec.Containers), "Number of containers should match")
	for _, expectedContainer := range expected.Spec.Containers {
		actualContainer := findContainer(actual.Spec.Containers, expectedContainer.Name)
		assert.NotNil(t, actualContainer, "Container %s should exist", expectedContainer.Name)
		if actualContainer != nil {
			// Check security context
			if expectedContainer.SecurityContext != nil {
				assert.NotNil(t, actualContainer.SecurityContext, "Security context should exist")
				if expectedContainer.SecurityContext.Capabilities != nil {
					assert.NotNil(t, actualContainer.SecurityContext.Capabilities, "Capabilities should exist")
					// Check each capability
					for _, cap := range expectedContainer.SecurityContext.Capabilities.Add {
						found := false
						for _, actualCap := range actualContainer.SecurityContext.Capabilities.Add {
							if cap == actualCap {
								found = true
								break
							}
						}
						assert.True(t, found, "Expected capability %s not found", cap)
					}
				}
			}

			// Check volume mounts
			assert.Equal(t, len(expectedContainer.VolumeMounts), len(actualContainer.VolumeMounts),
				"Number of volume mounts should match for container %s", expectedContainer.Name)
			for _, expectedMount := range expectedContainer.VolumeMounts {
				found := false
				for _, actualMount := range actualContainer.VolumeMounts {
					if expectedMount.Name == actualMount.Name {
						assert.Equal(t, expectedMount, actualMount, "Volume mount %s should match", expectedMount.Name)
						found = true
						break
					}
				}
				assert.True(t, found, "Expected volume mount %s not found in container %s",
					expectedMount.Name, expectedContainer.Name)
			}

			// Check environment variables without relying on order
			assert.Equal(t, len(expectedContainer.Env), len(actualContainer.Env),
				"Number of environment variables should match for container %s", expectedContainer.Name)
			for _, expectedEnv := range expectedContainer.Env {
				found := false
				for _, actualEnv := range actualContainer.Env {
					if expectedEnv.Name == actualEnv.Name {
						assert.Equal(t, expectedEnv.Value, actualEnv.Value,
							"Environment variable %s value should match", expectedEnv.Name)
						found = true
						break
					}
				}
				assert.True(t, found, "Expected environment variable %s not found in container %s",
					expectedEnv.Name, expectedContainer.Name)
			}
		}
	}
}

// Helper function to get expected environment variables for tests
func getExpectedEnvVars(t testing.TB) []v1.EnvVar {
	var envVars []v1.EnvVar
	for name, value := range fixtureRDMA(t).Profiles["roce"].EnvVars {
		envVars = append(envVars, v1.EnvVar{
			Name:  name,
			Value: value,
		})
	}
	return envVars
}

func TestRDMAInjector_injectNetworks(t *testing.T) {
	tests := []struct {
		name              string
		existingAnno      string
		profileNetworks   []NetworkAttachment
		expectErr         bool
		expectAnnoPresent bool
		expectAnnoValue   string
	}{
		{
			name:              "no_profile_networks_keeps_pod_untouched",
			existingAnno:      "",
			profileNetworks:   nil,
			expectAnnoPresent: false,
		},
		{
			name:              "writes_fresh_annotation",
			existingAnno:      "",
			profileNetworks:   []NetworkAttachment{{Name: "rdma0-macvlan", Namespace: "multus-system"}},
			expectAnnoPresent: true,
			expectAnnoValue:   `[{"name":"rdma0-macvlan","namespace":"multus-system"}]`,
		},
		{
			name:              "merges_with_existing_dedup_by_name_and_namespace",
			existingAnno:      `[{"name":"rdma0-macvlan","namespace":"multus-system"}]`,
			profileNetworks:   []NetworkAttachment{{Name: "rdma0-macvlan", Namespace: "multus-system"}, {Name: "rdma1-macvlan", Namespace: "multus-system"}},
			expectAnnoPresent: true,
			expectAnnoValue:   `[{"name":"rdma0-macvlan","namespace":"multus-system"},{"name":"rdma1-macvlan","namespace":"multus-system"}]`,
		},
		{
			name:            "malformed_existing_annotation_returns_error",
			existingAnno:    "not-json",
			profileNetworks: []NetworkAttachment{{Name: "rdma0-macvlan", Namespace: "multus-system"}},
			expectErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{}}
			if tt.existingAnno != "" {
				pod.Annotations = map[string]string{MultusNetworksAnnotation: tt.existingAnno}
			}
			err := injectNetworks(pod, tt.profileNetworks)
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			val, ok := pod.Annotations[MultusNetworksAnnotation]
			assert.Equal(t, tt.expectAnnoPresent, ok)
			if tt.expectAnnoPresent {
				assert.Equal(t, tt.expectAnnoValue, val)
			}
		})
	}
}

// TestRDMAInjector_injectContainerConfig_NilProfileSecurityContext verifies
// that a networks-only profile (no securityContext), such as the fixture's,
// does not panic or alter the container's existing security context.
func TestRDMAInjector_injectContainerConfig_NilProfileSecurityContext(t *testing.T) {
	tests := []struct {
		name            string
		securityContext *v1.SecurityContext
	}{
		{name: "container_without_security_context", securityContext: nil},
		{name: "container_with_nil_capabilities", securityContext: &v1.SecurityContext{}},
		{
			name: "container_with_capabilities",
			securityContext: &v1.SecurityContext{
				Capabilities: &v1.Capabilities{Add: []v1.Capability{"IPC_LOCK"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			container := &v1.Container{
				Name:            "ome-container",
				SecurityContext: tt.securityContext.DeepCopy(),
			}
			assert.NotPanics(t, func() {
				injectContainerConfig(container, fixtureRDMA(t).Profiles["multus-attachments"])
			})
			assert.Equal(t, tt.securityContext, container.SecurityContext,
				"networks-only profile must not change the security context")
		})
	}
}

// TestRDMAInjector_injectContainerConfig_EnvDedup verifies that env vars
// already present on the container win over profile values and that
// re-running the injector (webhook reinvocation) does not stack duplicates.
func TestRDMAInjector_injectContainerConfig_EnvDedup(t *testing.T) {
	profile := fixtureRDMA(t).Profiles["roce"]

	container := &v1.Container{
		Name: "ome-container",
		Env:  []v1.EnvVar{{Name: "NCCL_DEBUG", Value: "WARN"}},
	}

	injectContainerConfig(container, profile)
	injectContainerConfig(container, profile)

	assert.Len(t, container.Env, len(profile.EnvVars),
		"each env name must appear exactly once after repeated injection")
	count := 0
	for _, env := range container.Env {
		if env.Name == "NCCL_DEBUG" {
			count++
			assert.Equal(t, "WARN", env.Value, "pre-existing env value must be preserved")
		}
	}
	assert.Equal(t, 1, count, "NCCL_DEBUG must not be duplicated")
}
