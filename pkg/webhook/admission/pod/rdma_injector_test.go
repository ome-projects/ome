package pod

import (
	"strings"
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
							Env:  getExpectedEnvVars(),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      DevInfVolumeName,
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
							Name: DevInfVolumeName,
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
							Env:  getExpectedEnvVars(),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      DevInfVolumeName,
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
							Name: DevInfVolumeName,
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
							Env:  getExpectedEnvVars(),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      DevInfVolumeName,
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
							Name: DevInfVolumeName,
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
							Env:  getExpectedEnvVars(),
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
									Name:      DevInfVolumeName,
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
							Name: DevInfVolumeName,
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
						constants.RDMAProfileAnnotationKey:    "oci-roce", // Using existing profile since we only have one defined
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
						constants.RDMAProfileAnnotationKey:    "oci-roce",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Env:  getExpectedEnvVars(),
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      DshmVolumeName,
									MountPath: "/dev/shm",
								},
								{
									Name:      DevInfVolumeName,
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
							Name: DevInfVolumeName,
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
			err := injectRDMA(tt.pod)

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
							Name:      DevInfVolumeName,
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
					Name: DevInfVolumeName,
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/dev/infiniband",
						},
					},
				},
			},
		},
	}

	profile := RDMAProfiles["oci-roce"]
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
func getExpectedEnvVars() []v1.EnvVar {
	var envVars []v1.EnvVar
	for name, value := range RDMAProfiles["oci-roce"].EnvVars {
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
			profileNetworks:   []NetworkAttachment{{Name: "ibs0p0-macvlan", Namespace: "cw-multus"}},
			expectAnnoPresent: true,
			expectAnnoValue:   `[{"name":"ibs0p0-macvlan","namespace":"cw-multus"}]`,
		},
		{
			name:              "merges_with_existing_dedup_by_name_and_namespace",
			existingAnno:      `[{"name":"ibs0p0-macvlan","namespace":"cw-multus"}]`,
			profileNetworks:   []NetworkAttachment{{Name: "ibs0p0-macvlan", Namespace: "cw-multus"}, {Name: "ibs0p1-macvlan", Namespace: "cw-multus"}},
			expectAnnoPresent: true,
			expectAnnoValue:   `[{"name":"ibs0p0-macvlan","namespace":"cw-multus"},{"name":"ibs0p1-macvlan","namespace":"cw-multus"}]`,
		},
		{
			name:            "malformed_existing_annotation_returns_error",
			existingAnno:    "not-json",
			profileNetworks: []NetworkAttachment{{Name: "ibs0p0-macvlan", Namespace: "cw-multus"}},
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

func TestRDMAProfiles_cksGbRdmaHasAll16Macvlans(t *testing.T) {
	profile, ok := RDMAProfiles["cks-gb-rdma"]
	assert.True(t, ok, "cks-gb-rdma profile should be registered")
	assert.Len(t, profile.Networks, 16, "cks-gb-rdma should have 4 HCAs × 4 ports")
	// First and last entries should match the (hca, port) generation order.
	assert.Equal(t, NetworkAttachment{Name: "ibs0p0-macvlan", Namespace: "cw-multus"}, profile.Networks[0])
	assert.Equal(t, NetworkAttachment{Name: "ibs3p3-macvlan", Namespace: "cw-multus"}, profile.Networks[15])
	// Profile is networks-only — workload knobs stay in the runtime YAML.
	assert.Empty(t, profile.EnvVars)
	assert.Empty(t, profile.Volumes)
	assert.Nil(t, profile.SecurityContext)
}

// TestRDMAInjector_injectContainerConfig_NilProfileSecurityContext verifies
// that a networks-only profile (no securityContext, e.g. cks-gb-rdma)
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
				injectContainerConfig(container, RDMAProfiles["cks-gb-rdma"])
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
	profile := RDMAProfiles["oci-roce"]

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

// A privileged container bypasses the device cgroup and sees every GPU on
// the node instead of the ones the NVIDIA device plugin allocated to it, so
// no shipped profile may ask for it. Capability names are checked here too:
// Kubernetes spells them without the CAP_ prefix, and a misspelled one is
// silently useless once the container is no longer privileged.
func TestRDMAProfiles_AreNotPrivileged(t *testing.T) {
	for name, profile := range RDMAProfiles {
		if profile.SecurityContext == nil {
			continue
		}
		assert.Nil(t, profile.SecurityContext.Privileged,
			"profile %q sets privileged, which defeats GPU device isolation", name)
		if profile.SecurityContext.Capabilities == nil {
			continue
		}
		for _, capability := range profile.SecurityContext.Capabilities.Add {
			assert.False(t, strings.HasPrefix(string(capability), "CAP_"),
				"profile %q adds %q; Kubernetes capability names omit the CAP_ prefix", name, capability)
		}
	}
}

// UCX_TLS and UCX_NET_DEVICES decide which transports and devices UCX may
// use. NCCL's built-in IB transport never reads them, so a profile gains
// nothing by pinning them, while UCX-based KV transfer such as NIXL quietly
// falls back to whatever they allow. Runtimes that want a restriction set it
// on the container.
func TestRDMAProfiles_EveryProfile_LeavesUCXTransportEnvUnset(t *testing.T) {
	for name, profile := range RDMAProfiles {
		for _, key := range []string{"UCX_TLS", "UCX_NET_DEVICES"} {
			_, has := profile.EnvVars[key]
			assert.False(t, has,
				"profile %q sets %s, which overrides UCX transport selection for every UCX user in the pod", name, key)
		}
	}
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
	// every GPU on the node instead of its allocated ones.
	assert.Nil(t, p.SecurityContext.Privileged)
}
