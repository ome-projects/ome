package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestNewPodSpecReconciler(t *testing.T) {
	reconciler := &PodSpecReconciler{
		Log: ctrl.Log.WithName("test"),
	}

	assert.NotNil(t, reconciler)
	assert.NotNil(t, reconciler.Log)
}

func TestPodSpecReconciler_ReconcilePodSpec(t *testing.T) {
	tests := []struct {
		name          string
		isvc          *v1beta1.InferenceService
		basePodSpec   *v1beta1.PodSpec
		runnerSpec    *v1beta1.RunnerSpec
		objectMeta    *metav1.ObjectMeta
		expectedError bool
		verifyFunc    func(t *testing.T, podSpec *v1.PodSpec)
	}{
		{
			name: "Basic pod spec reconciliation",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "test-image:latest",
					},
				},
			},
			runnerSpec: &v1beta1.RunnerSpec{
				Container: v1.Container{
					Name: constants.MainContainerName,
					Env: []v1.EnvVar{
						{Name: "CUSTOM_ENV", Value: "custom"},
					},
				},
			},
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				assert.Len(t, podSpec.Containers, 1)
				assert.Equal(t, constants.MainContainerName, podSpec.Containers[0].Name)
				assert.Equal(t, "test-image:latest", podSpec.Containers[0].Image)
				// Check that custom env was added
				envFound := false
				for _, env := range podSpec.Containers[0].Env {
					if env.Name == "CUSTOM_ENV" && env.Value == "custom" {
						envFound = true
						break
					}
				}
				assert.True(t, envFound, "Custom env var not found")
			},
		},
		{
			name: "Pod spec with volumes and affinity",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "test-image:latest",
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "test-volume",
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: "test-config"},
							},
						},
					},
				},
				NodeSelector: map[string]string{
					"gpu": "true",
				},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "node-type",
											Operator: v1.NodeSelectorOpIn,
											Values:   []string{"gpu-node"},
										},
									},
								},
							},
						},
					},
				},
			},
			runnerSpec: nil,
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				// Check volumes
				assert.Len(t, podSpec.Volumes, 1)
				assert.Equal(t, "test-volume", podSpec.Volumes[0].Name)
				// Check node selector
				assert.Equal(t, "true", podSpec.NodeSelector["gpu"])
				// Check affinity
				assert.NotNil(t, podSpec.Affinity)
				assert.NotNil(t, podSpec.Affinity.NodeAffinity)
			},
		},
		{
			name: "Pod spec with tolerations and image pull secrets",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "test-image:latest",
					},
				},
				Tolerations: []v1.Toleration{
					{
						Key:      "gpu",
						Operator: v1.TolerationOpEqual,
						Value:    "true",
						Effect:   v1.TaintEffectNoSchedule,
					},
				},
				ImagePullSecrets: []v1.LocalObjectReference{
					{Name: "docker-secret"},
				},
			},
			runnerSpec: nil,
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				// Check tolerations
				assert.Len(t, podSpec.Tolerations, 1)
				assert.Equal(t, "gpu", podSpec.Tolerations[0].Key)
				// Check image pull secrets
				assert.Len(t, podSpec.ImagePullSecrets, 1)
				assert.Equal(t, "docker-secret", podSpec.ImagePullSecrets[0].Name)
			},
		},
		{
			name: "Pod spec with multiple containers",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "test-image:latest",
					},
					{
						Name:  "sidecar",
						Image: "sidecar-image:latest",
					},
				},
			},
			runnerSpec: &v1beta1.RunnerSpec{
				Container: v1.Container{
					Name: constants.MainContainerName,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				assert.Len(t, podSpec.Containers, 2)

				// Check main container
				mainContainer := findContainer(podSpec.Containers, constants.MainContainerName)
				require.NotNil(t, mainContainer)
				assert.Equal(t, "test-image:latest", mainContainer.Image)
				assert.Equal(t, resource.MustParse("1"), mainContainer.Resources.Requests[v1.ResourceCPU])

				// Check sidecar container
				sidecarContainer := findContainer(podSpec.Containers, "sidecar")
				require.NotNil(t, sidecarContainer)
				assert.Equal(t, "sidecar-image:latest", sidecarContainer.Image)
			},
		},
		{
			name: "Pod spec with scheduler and DNS settings",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "test-image:latest",
					},
				},
				SchedulerName: "custom-scheduler",
				HostNetwork:   true,
				HostIPC:       true,
				DNSPolicy:     v1.DNSClusterFirstWithHostNet,
			},
			runnerSpec: nil,
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				assert.Equal(t, "custom-scheduler", podSpec.SchedulerName)
				assert.True(t, podSpec.HostNetwork)
				assert.True(t, podSpec.HostIPC)
				assert.Equal(t, v1.DNSClusterFirstWithHostNet, podSpec.DNSPolicy)
			},
		},
		{
			name: "Pod spec with runner container merge",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "test-image:latest",
						Env: []v1.EnvVar{
							{Name: "RUNTIME_ENV", Value: "runtime"},
						},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("500m"),
							},
						},
					},
				},
			},
			runnerSpec: &v1beta1.RunnerSpec{
				Container: v1.Container{
					Name:    constants.MainContainerName,
					Command: []string{"/bin/bash"},
					Args:    []string{"-c", "echo hello"},
					Env: []v1.EnvVar{
						{Name: "CUSTOM_ENV", Value: "custom"},
					},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("2Gi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
			},
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				assert.Len(t, podSpec.Containers, 1)

				container := &podSpec.Containers[0]
				// Check that command and args are preserved
				assert.Equal(t, []string{"/bin/bash"}, container.Command)
				assert.Equal(t, []string{"-c", "echo hello"}, container.Args)

				// Check env vars - both should be present
				envMap := make(map[string]string)
				for _, env := range container.Env {
					envMap[env.Name] = env.Value
				}
				assert.Equal(t, "runtime", envMap["RUNTIME_ENV"])
				assert.Equal(t, "custom", envMap["CUSTOM_ENV"])

				// Check resources - should be merged
				assert.Equal(t, resource.MustParse("500m"), container.Resources.Requests[v1.ResourceCPU])
				assert.Equal(t, resource.MustParse("2Gi"), container.Resources.Requests[v1.ResourceMemory])
				assert.Equal(t, resource.MustParse("4Gi"), container.Resources.Limits[v1.ResourceMemory])
			},
		},
		{
			name: "No containers in pod spec",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{},
			},
			runnerSpec: &v1beta1.RunnerSpec{
				Container: v1.Container{
					Name:  constants.MainContainerName,
					Image: "main-image:latest",
				},
			},
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				assert.Len(t, podSpec.Containers, 1)

				// Check that runner container was added
				mainContainer := findContainer(podSpec.Containers, constants.MainContainerName)
				require.NotNil(t, mainContainer)
				assert.Equal(t, "main-image:latest", mainContainer.Image)
			},
		},
		{
			name: "No containers and no runner spec",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			basePodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{},
			},
			runnerSpec: nil,
			objectMeta: &metav1.ObjectMeta{
				Name: "test-pod",
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &PodSpecReconciler{
				Log: ctrl.Log.WithName("test"),
			}

			podSpec, err := reconciler.ReconcilePodSpec(
				tt.isvc,
				tt.objectMeta,
				tt.basePodSpec,
				tt.runnerSpec,
			)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.verifyFunc != nil {
				tt.verifyFunc(t, podSpec)
			}
		})
	}
}

func TestPodSpecReconciler_ReconcileWorkerPodSpec(t *testing.T) {
	tests := []struct {
		name             string
		isvc             *v1beta1.InferenceService
		workerPodSpec    *v1beta1.PodSpec
		leaderRunnerSpec *v1beta1.RunnerSpec
		objectMeta       *metav1.ObjectMeta
		expectedError    bool
		verifyFunc       func(t *testing.T, podSpec *v1.PodSpec)
	}{
		{
			name: "No worker pod spec",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			workerPodSpec:    nil,
			leaderRunnerSpec: nil,
			objectMeta: &metav1.ObjectMeta{
				Name: "test-worker",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				assert.Nil(t, podSpec)
			},
		},
		{
			name: "Worker pod spec without leader runner spec",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			workerPodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "worker-image:latest",
					},
				},
				NodeSelector: map[string]string{
					"worker": "true",
				},
			},
			leaderRunnerSpec: nil,
			objectMeta: &metav1.ObjectMeta{
				Name: "test-worker",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				assert.Len(t, podSpec.Containers, 1)
				assert.Equal(t, "worker-image:latest", podSpec.Containers[0].Image)
				assert.Equal(t, "true", podSpec.NodeSelector["worker"])
			},
		},
		{
			name: "Worker pod spec with leader runner spec",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			workerPodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "worker-image:latest",
						Env: []v1.EnvVar{
							{Name: "RUNTIME_ENV", Value: "runtime"},
						},
					},
				},
			},
			leaderRunnerSpec: &v1beta1.RunnerSpec{
				Container: v1.Container{
					Name: constants.MainContainerName,
					Env: []v1.EnvVar{
						{Name: "LEADER_ENV", Value: "leader"},
					},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			objectMeta: &metav1.ObjectMeta{
				Name: "test-worker",
			},
			expectedError: false,
			verifyFunc: func(t *testing.T, podSpec *v1.PodSpec) {
				require.NotNil(t, podSpec)
				assert.Len(t, podSpec.Containers, 1)

				container := &podSpec.Containers[0]
				assert.Equal(t, "worker-image:latest", container.Image)

				// Check env vars - both should be present
				envMap := make(map[string]string)
				for _, env := range container.Env {
					envMap[env.Name] = env.Value
				}
				assert.Equal(t, "runtime", envMap["RUNTIME_ENV"])
				assert.Equal(t, "leader", envMap["LEADER_ENV"])

				// Check resources
				assert.Equal(t, resource.MustParse("2"), container.Resources.Requests[v1.ResourceCPU])
			},
		},
		{
			name: "Worker pod spec with no containers",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			workerPodSpec: &v1beta1.PodSpec{
				Containers: []v1.Container{},
			},
			leaderRunnerSpec: nil,
			objectMeta: &metav1.ObjectMeta{
				Name: "test-worker",
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &PodSpecReconciler{
				Log: ctrl.Log.WithName("test"),
			}

			podSpec, err := reconciler.ReconcileWorkerPodSpec(
				tt.isvc,
				tt.objectMeta,
				tt.workerPodSpec,
				tt.leaderRunnerSpec,
			)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.verifyFunc != nil {
				tt.verifyFunc(t, podSpec)
			}
		})
	}
}

// Helper function to find container by name
func findContainer(containers []v1.Container, name string) *v1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

// TestPodSpecReconciler_ImagePrecedence pins the merge contract: the
// user-supplied ISVC container image overrides the CSR runner image
// when both are set, and the CSR runner image fills in when the ISVC
// container omits image (Container.Image has omitempty so an unset
// value is dropped from the strategic-merge payload).
func TestPodSpecReconciler_ImagePrecedence(t *testing.T) {
	tests := []struct {
		name          string
		isvcImage     string
		runnerImage   string
		expectedImage string
		why           string
	}{
		{
			name:          "ISVC image set: ISVC wins",
			isvcImage:     "user/pinned:v42",
			runnerImage:   "runtime/default:v1",
			expectedImage: "user/pinned:v42",
			why:           "user-supplied ISVC image is the strategic-merge override and must win",
		},
		{
			name:          "ISVC image empty: CSR runner image fills in",
			isvcImage:     "",
			runnerImage:   "runtime/default:v1",
			expectedImage: "runtime/default:v1",
			why:           "Container.Image is omitempty; empty ISVC value drops from the merge payload so runtime base survives",
		},
		{
			name:          "Both empty: rendered pod also has empty image",
			isvcImage:     "",
			runnerImage:   "",
			expectedImage: "",
			why:           "no source of truth for image — controller does not synthesize one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &PodSpecReconciler{Log: ctrl.Log.WithName("test")}
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			}
			basePodSpec := &v1beta1.PodSpec{
				Containers: []v1.Container{{
					Name:  constants.MainContainerName,
					Image: tt.isvcImage,
				}},
			}
			runnerSpec := &v1beta1.RunnerSpec{
				Container: v1.Container{
					Name:  constants.MainContainerName,
					Image: tt.runnerImage,
				},
			}
			podSpec, err := r.ReconcilePodSpec(isvc, &metav1.ObjectMeta{}, basePodSpec, runnerSpec)
			require.NoError(t, err)
			require.Len(t, podSpec.Containers, 1)
			assert.Equal(t, tt.expectedImage, podSpec.Containers[0].Image, tt.why)
		})
	}
}
