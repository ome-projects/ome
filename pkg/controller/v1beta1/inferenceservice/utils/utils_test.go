package utils

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestIsMergedFineTunedWeight(t *testing.T) {
	tests := []struct {
		name            string
		fineTunedWeight *v1beta1.FineTunedWeight
		expectedResult  bool
		expectError     bool
	}{
		{
			name: "merged weights true",
			fineTunedWeight: &v1beta1.FineTunedWeight{
				Spec: v1beta1.FineTunedWeightSpec{
					Configuration: runtime.RawExtension{
						Raw: marshalJSONHelper(map[string]interface{}{
							constants.FineTunedWeightMergedWeightsConfigKey: true,
						}),
					},
				},
			},
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "merged weights false",
			fineTunedWeight: &v1beta1.FineTunedWeight{
				Spec: v1beta1.FineTunedWeightSpec{
					Configuration: runtime.RawExtension{
						Raw: marshalJSONHelper(map[string]interface{}{
							constants.FineTunedWeightMergedWeightsConfigKey: false,
						}),
					},
				},
			},
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "merged weights key not present",
			fineTunedWeight: &v1beta1.FineTunedWeight{
				Spec: v1beta1.FineTunedWeightSpec{
					Configuration: runtime.RawExtension{
						Raw: marshalJSONHelper(map[string]interface{}{
							"other_config": "value",
						}),
					},
				},
			},
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "invalid json configuration",
			fineTunedWeight: &v1beta1.FineTunedWeight{
				Spec: v1beta1.FineTunedWeightSpec{
					Configuration: runtime.RawExtension{
						Raw: []byte(`{invalid json`),
					},
				},
			},
			expectedResult: false,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := IsMergedFineTunedWeight(tt.fineTunedWeight)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestIsEmptyModelDirVolumeRequired(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    bool
	}{
		{
			name:        "no annotations",
			annotations: map[string]string{},
			expected:    false,
		},
		{
			name: "fine tuned adapter injection annotation present",
			annotations: map[string]string{
				constants.FineTunedAdapterInjectionKey: "amaaaaaask7dceya3ro4ls2wit3tu5dkk2u2ijvbbu4gmhbrsjeytwc2yagq",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsEmptyModelDirVolumeRequired(tt.annotations)
			assert.Equal(t, tt.expected, result, "Test case: %s", tt.name)
		})
	}
}

func TestIsOriginalModelVolumeMountNecessary(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    bool
	}{
		{
			name:        "empty annotations",
			annotations: map[string]string{},
			expected:    true,
		},
		{
			name: "ft serving with merged weights true",
			annotations: map[string]string{
				constants.FTServingWithMergedWeightsAnnotationKey: "true",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsOriginalModelVolumeMountNecessary(tt.annotations)
			assert.Equal(t, tt.expected, result, "Test case: %s", tt.name)
		})
	}
}

func TestIsCohereCommand1TFewFTServing(t *testing.T) {
	tests := []struct {
		name       string
		objectMeta *metav1.ObjectMeta
		expected   bool
	}{
		{
			name: "cohere command 1 TFew FT serving",
			objectMeta: &metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.BaseModelVendorAnnotationKey: string(constants.Cohere),
					constants.FineTunedWeightFTStrategyKey: string(constants.TFewTrainingStrategy),
				},
			},
			expected: true,
		},
		{
			name: "all conditions not met - Llama LoRA FT Serving",
			objectMeta: &metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.BaseModelVendorAnnotationKey:            string(constants.Meta),
					constants.FineTunedWeightFTStrategyKey:            string(constants.LoraTrainingStrategy),
					constants.FTServingWithMergedWeightsAnnotationKey: "true",
				},
			},
			expected: false,
		},
		{
			name: "not matched strategy plus merged weights - Cohere Command R LoRA FT Serving",
			objectMeta: &metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.BaseModelVendorAnnotationKey:            string(constants.Cohere),
					constants.FineTunedWeightFTStrategyKey:            string(constants.LoraTrainingStrategy),
					constants.FTServingWithMergedWeightsAnnotationKey: "true",
				},
			},
			expected: false,
		},
		{
			name: "Cohere Command R TFew FT Serving",
			objectMeta: &metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.BaseModelVendorAnnotationKey:            string(constants.Cohere),
					constants.FineTunedWeightFTStrategyKey:            string(constants.TFewTrainingStrategy),
					constants.FTServingWithMergedWeightsAnnotationKey: "true",
				},
			},
			expected: false,
		},
		{
			name: "missing FT strategy annotation",
			objectMeta: &metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.BaseModelVendorAnnotationKey:            string(constants.Cohere),
					constants.FTServingWithMergedWeightsAnnotationKey: "false",
				},
			},
			expected: false,
		},
		{
			name:       "empty annotations",
			objectMeta: &metav1.ObjectMeta{},
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCohereCommand1TFewFTServing(tt.objectMeta)
			assert.Equal(t, tt.expected, result, "Test case: %s", tt.name)
		})
	}
}

func TestUpdateVolumeMount(t *testing.T) {
	tests := []struct {
		name           string
		container      *v1.Container
		volumeMount    *v1.VolumeMount
		expectedMounts []v1.VolumeMount
	}{
		{
			name: "update existing volume mount",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "test-volume",
						MountPath: "/old/path",
						SubPath:   "old-sub-path",
						ReadOnly:  false,
					},
				},
			},
			volumeMount: &v1.VolumeMount{
				Name:      "test-volume",
				MountPath: "/new/path",
				SubPath:   "new-sub-path",
				ReadOnly:  true,
			},
			expectedMounts: []v1.VolumeMount{
				{
					Name:      "test-volume",
					MountPath: "/new/path",
					SubPath:   "new-sub-path",
					ReadOnly:  true,
				},
			},
		},
		{
			name: "add new volume mount",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "existing-volume",
						MountPath: "/existing/path",
					},
				},
			},
			volumeMount: &v1.VolumeMount{
				Name:      "new-volume",
				MountPath: "/new/path",
				SubPath:   "new-sub-path",
				ReadOnly:  true,
			},
			expectedMounts: []v1.VolumeMount{
				{
					Name:      "existing-volume",
					MountPath: "/existing/path",
				},
				{
					Name:      "new-volume",
					MountPath: "/new/path",
					SubPath:   "new-sub-path",
					ReadOnly:  true,
				},
			},
		},
		{
			name: "nil volume mount",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "existing-volume",
						MountPath: "/existing/path",
					},
				},
			},
			volumeMount: nil,
			expectedMounts: []v1.VolumeMount{
				{
					Name:      "existing-volume",
					MountPath: "/existing/path",
				},
			},
		},
		{
			name: "update one of multiple volume mounts",
			container: &v1.Container{
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      "volume-1",
						MountPath: "/path/1",
					},
					{
						Name:      "volume-2",
						MountPath: "/old/path",
						SubPath:   "old-sub-path",
					},
					{
						Name:      "volume-3",
						MountPath: "/path/3",
					},
				},
			},
			volumeMount: &v1.VolumeMount{
				Name:      "volume-2",
				MountPath: "/new/path",
				SubPath:   "new-sub-path",
				ReadOnly:  true,
			},
			expectedMounts: []v1.VolumeMount{
				{
					Name:      "volume-1",
					MountPath: "/path/1",
				},
				{
					Name:      "volume-2",
					MountPath: "/new/path",
					SubPath:   "new-sub-path",
					ReadOnly:  true,
				},
				{
					Name:      "volume-3",
					MountPath: "/path/3",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			UpdateVolumeMount(tt.container, tt.volumeMount)
			assert.Equal(t, tt.expectedMounts, tt.container.VolumeMounts, "Test case: %s", tt.name)
		})
	}
}

func TestUpdateEnvVars(t *testing.T) {
	tests := []struct {
		name        string
		container   *v1.Container
		envVar      *v1.EnvVar
		expectedEnv []v1.EnvVar
	}{
		{
			name: "update existing env var",
			container: &v1.Container{
				Env: []v1.EnvVar{
					{
						Name:  "TEST_VAR",
						Value: "old-value",
					},
				},
			},
			envVar: &v1.EnvVar{
				Name:  "TEST_VAR",
				Value: "new-value",
			},
			expectedEnv: []v1.EnvVar{
				{
					Name:  "TEST_VAR",
					Value: "new-value",
				},
			},
		},
		{
			name: "replace sourced env var",
			container: &v1.Container{
				Env: []v1.EnvVar{{
					Name: "TEST_VAR",
					ValueFrom: &v1.EnvVarSource{
						ConfigMapKeyRef: &v1.ConfigMapKeySelector{
							LocalObjectReference: v1.LocalObjectReference{Name: "settings"},
							Key:                  "value",
						},
					},
				}},
			},
			envVar: &v1.EnvVar{Name: "TEST_VAR", Value: "new-value"},
			expectedEnv: []v1.EnvVar{
				{Name: "TEST_VAR", Value: "new-value"},
			},
		},
		{
			name: "add new env var",
			container: &v1.Container{
				Env: []v1.EnvVar{
					{
						Name:  "EXISTING_VAR",
						Value: "existing-value",
					},
				},
			},
			envVar: &v1.EnvVar{
				Name:  "NEW_VAR",
				Value: "new-value",
			},
			expectedEnv: []v1.EnvVar{
				{
					Name:  "EXISTING_VAR",
					Value: "existing-value",
				},
				{
					Name:  "NEW_VAR",
					Value: "new-value",
				},
			},
		},
		{
			name: "update one of multiple env vars",
			container: &v1.Container{
				Env: []v1.EnvVar{
					{
						Name:  "VAR1",
						Value: "value1",
					},
					{
						Name:  "VAR2",
						Value: "old-value2",
					},
					{
						Name:  "VAR3",
						Value: "value3",
					},
				},
			},
			envVar: &v1.EnvVar{
				Name:  "VAR2",
				Value: "new-value2",
			},
			expectedEnv: []v1.EnvVar{
				{
					Name:  "VAR1",
					Value: "value1",
				},
				{
					Name:  "VAR2",
					Value: "new-value2",
				},
				{
					Name:  "VAR3",
					Value: "value3",
				},
			},
		},
		{
			name:      "empty container env",
			container: &v1.Container{},
			envVar: &v1.EnvVar{
				Name:  "NEW_VAR",
				Value: "new-value",
			},
			expectedEnv: []v1.EnvVar{
				{
					Name:  "NEW_VAR",
					Value: "new-value",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			UpdateEnvVars(tt.container, tt.envVar)
			assert.Equal(t, tt.expectedEnv, tt.container.Env, "Test case: %s", tt.name)
		})
	}
}

// Helper function to marshal JSON and panic on error (for test data setup only)
func marshalJSONHelper(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func TestMergeRouterSpec(t *testing.T) {
	// Create a sample RouterSpec for use in tests
	isvcRouter := &v1beta1.RouterSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "isvc-container",
					Image: "isvc-image",
				},
			},
		},
	}

	runtimeRouter := &v1beta1.RouterSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "runtime-container",
					Image: "runtime-image",
				},
			},
		},
	}

	tests := []struct {
		name          string
		isvcRouter    *v1beta1.RouterSpec
		runtimeRouter *v1beta1.RouterSpec
		expected      *v1beta1.RouterSpec
		expectError   bool
	}{
		{
			name:          "isvc router is nil",
			isvcRouter:    nil,
			runtimeRouter: runtimeRouter,
			expected:      nil,
			expectError:   false,
		},
		{
			name:          "runtime router is nil",
			isvcRouter:    isvcRouter,
			runtimeRouter: nil,
			expected:      isvcRouter,
			expectError:   false,
		},
		{
			name:          "both routers are nil",
			isvcRouter:    nil,
			runtimeRouter: nil,
			expected:      nil,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, err := MergeRouterSpec(tt.isvcRouter, tt.runtimeRouter)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, merged)
			}
		})
	}
}

func TestMergeEngineSpec(t *testing.T) {
	intPtr := func(i int) *int { return &i }
	int64Ptr := func(i int64) *int64 { return &i }

	tests := []struct {
		name           string
		runtimeEngine  *v1beta1.EngineSpec
		isvcEngine     *v1beta1.EngineSpec
		expectedEngine *v1beta1.EngineSpec
		expectError    bool
	}{
		{
			name:           "both nil",
			runtimeEngine:  nil,
			isvcEngine:     nil,
			expectedEngine: nil,
			expectError:    false,
		},
		{
			name:          "runtime nil, isvc not nil",
			runtimeEngine: nil,
			isvcEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(2),
					MaxReplicas: 5,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "engine:latest",
						},
					},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(2),
					MaxReplicas: 5,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "engine:latest",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "runtime not nil, isvc nil",
			runtimeEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 3,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "runtime-engine:v1",
						},
					},
				},
			},
			isvcEngine:     nil,
			expectedEngine: nil,
			expectError:    false,
		},
		{
			name: "merge min/max replicas - isvc overrides",
			runtimeEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 3,
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(2),
					MaxReplicas: 10,
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(2),
					MaxReplicas: 10,
				},
			},
			expectError: false,
		},
		{
			name: "merge containers - isvc overrides",
			runtimeEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "runtime:v1",
							Env: []v1.EnvVar{
								{Name: "ENV1", Value: "runtime-value"},
								{Name: "ENV2", Value: "runtime-value2"},
							},
						},
						{
							Name:  "sidecar",
							Image: "sidecar:v1",
						},
					},
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "isvc:v2",
							Env: []v1.EnvVar{
								{Name: "ENV1", Value: "isvc-value"},
								{Name: "ENV3", Value: "isvc-value3"},
							},
						},
					},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "isvc:v2",
							Env: []v1.EnvVar{
								{Name: "ENV1", Value: "isvc-value"},
								{Name: "ENV3", Value: "isvc-value3"},
								{Name: "ENV2", Value: "runtime-value2"},
							},
						},
						{
							Name:  "sidecar",
							Image: "sidecar:v1",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "merge runner spec",
			runtimeEngine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "runtime-runner",
						Image: "runtime-runner:v1",
						Args:  []string{"--arg1", "runtime"},
					},
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "isvc-runner",
						Image: "isvc-runner:v2",
						Args:  []string{"--arg1", "isvc", "--arg2", "new"},
					},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "isvc-runner",
						Image: "isvc-runner:v2",
						Args:  []string{"--arg1", "isvc", "--arg2", "new"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "merge leader and worker specs",
			runtimeEngine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "leader",
								Image: "runtime-leader:v1",
							},
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker",
								Image: "runtime-worker:v1",
							},
						},
					},
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "leader",
								Image: "isvc-leader:v2",
							},
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(4),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker",
								Image: "isvc-worker:v2",
							},
						},
					},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "leader",
								Image: "isvc-leader:v2",
							},
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(4),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker",
								Image: "isvc-worker:v2",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "merge pod spec fields - volumes, nodeSelector, tolerations",
			runtimeEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "model-volume",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/models",
								},
							},
						},
					},
					NodeSelector: map[string]string{
						"gpu":  "true",
						"zone": "us-west-1a",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "gpu",
							Operator: v1.TolerationOpEqual,
							Value:    "true",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "cache-volume",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
					},
					NodeSelector: map[string]string{
						"zone": "us-west-1b",
						"type": "inference",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "inference",
							Operator: v1.TolerationOpEqual,
							Value:    "true",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "cache-volume",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "model-volume",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/models",
								},
							},
						},
					},
					NodeSelector: map[string]string{
						"gpu":  "true",
						"zone": "us-west-1b",
						"type": "inference",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "inference",
							Operator: v1.TolerationOpEqual,
							Value:    "true",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "complex merge scenario - partial overrides",
			runtimeEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:    intPtr(1),
					MaxReplicas:    5,
					TimeoutSeconds: int64Ptr(50),
				},
				PodSpec: v1beta1.PodSpec{
					ServiceAccountName: "runtime-sa",
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "runtime:v1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("2Gi"),
								},
							},
						},
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
												Values:   []string{"gpu"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MaxReplicas:    10,
					TimeoutSeconds: int64Ptr(80),
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("2"),
									v1.ResourceMemory: resource.MustParse("2Gi"),
									"nvidia.com/gpu":  resource.MustParse("1"),
								},
								Limits: v1.ResourceList{
									"nvidia.com/gpu": resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:    intPtr(1),
					MaxReplicas:    10,
					TimeoutSeconds: int64Ptr(80),
				},
				PodSpec: v1beta1.PodSpec{
					ServiceAccountName: "runtime-sa",
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "runtime:v1",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("2"),
									v1.ResourceMemory: resource.MustParse("2Gi"),
									"nvidia.com/gpu":  resource.MustParse("1"),
								},
								Limits: v1.ResourceList{
									"nvidia.com/gpu": resource.MustParse("1"),
								},
							},
						},
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
												Values:   []string{"gpu"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "merge with nil fields in runtime",
			runtimeEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MaxReplicas: 5,
				},
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "runner",
						Image: "runner:latest",
					},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 5,
				},
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "runner",
						Image: "runner:latest",
					},
				},
			},
			expectError: false,
		},
		{
			name: "merge with empty containers in isvc overriding runtime",
			runtimeEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "runtime-container",
							Image: "runtime:v1",
						},
					},
				},
			},
			isvcEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{},
				},
			},
			expectedEngine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "runtime-container",
							Image: "runtime:v1",
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeEngineSpec(tt.runtimeEngine, tt.isvcEngine)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedEngine, result)
			}
		})
	}
}

// TestMergeRunnerName_RestoresFromRuntimeWhenISVCNameEmpty pins the
// fix: when a user patches only the runner image
// (kubectl-defaulted zero values for unmentioned fields produce
// `runner: {image: ..., name: "", resources: {}}` on the ISVC), the
// merged runner MUST keep the runtime's container name. Otherwise the
// rendered pod has `spec.containers[0].name = ""` and apiserver
// rejects the create with `spec.containers[0].name: Required value`,
// deadlocking the OMENative rollout in step=Drain.
//
// Covers all three component paths (engine/decoder/router) so the
// fix can't regress for any single one.
func TestMergeRunnerName_RestoresFromRuntimeWhenISVCNameEmpty(t *testing.T) {
	t.Run("engine", func(t *testing.T) {
		runtime := &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container", Image: "runtime:v1"}}}
		isvc := &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Image: "user:v2"}}}
		merged, err := MergeEngineSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.Equal(t, "ome-container", merged.Runner.Name, "container name must come from runtime when ISVC didn't set it")
		assert.Equal(t, "user:v2", merged.Runner.Image, "user's image override must still apply")
	})
	t.Run("decoder", func(t *testing.T) {
		runtime := &v1beta1.DecoderSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "decoder-runner", Image: "runtime:v1"}}}
		isvc := &v1beta1.DecoderSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Image: "user:v2"}}}
		merged, err := MergeDecoderSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.Equal(t, "decoder-runner", merged.Runner.Name)
		assert.Equal(t, "user:v2", merged.Runner.Image)
	})
	t.Run("router", func(t *testing.T) {
		runtime := &v1beta1.RouterSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "router-runner", Image: "runtime:v1"}}}
		isvc := &v1beta1.RouterSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Image: "user:v2"}}}
		merged, err := MergeRouterSpec(isvc, runtime)
		assert.NoError(t, err)
		assert.Equal(t, "router-runner", merged.Runner.Name)
		assert.Equal(t, "user:v2", merged.Runner.Image)
	})
	t.Run("explicit-name-still-wins", func(t *testing.T) {
		// Defensive: if the user DID set a non-empty name, the merge
		// must honor it (no surprise restore).
		runtime := &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "runtime-name", Image: "runtime:v1"}}}
		isvc := &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "user-name", Image: "user:v2"}}}
		merged, err := MergeEngineSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.Equal(t, "user-name", merged.Runner.Name, "explicit user name must override runtime name")
	})
}

// TestMergeTopologyKey_Resolution pins the effective-topologyKey rule for
// the gang co-location field: the ISVC component value (spec.engine /
// spec.decoder topologyKey) wins when set, otherwise the runtime
// component-config value (engineConfig / decoderConfig topologyKey) is
// inherited; unset on both stays nil (opt-in, zero behavior change).
func TestMergeTopologyKey_Resolution(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	t.Run("engine-isvc-overrides-runtime", func(t *testing.T) {
		runtime := &v1beta1.EngineSpec{TopologyKey: strPtr("runtime.domain/key")}
		isvc := &v1beta1.EngineSpec{TopologyKey: strPtr("isvc.domain/key")}
		merged, err := MergeEngineSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologyKey)
		assert.Equal(t, "isvc.domain/key", *merged.TopologyKey, "ISVC topologyKey must override the runtime value")
	})
	t.Run("engine-inherits-runtime-when-isvc-unset", func(t *testing.T) {
		runtime := &v1beta1.EngineSpec{TopologyKey: strPtr("topology.example.com/domain")}
		isvc := &v1beta1.EngineSpec{} // no topologyKey
		merged, err := MergeEngineSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologyKey, "runtime topologyKey must be inherited when ISVC leaves it unset")
		assert.Equal(t, "topology.example.com/domain", *merged.TopologyKey)
	})
	t.Run("engine-nil-on-both", func(t *testing.T) {
		merged, err := MergeEngineSpec(&v1beta1.EngineSpec{}, &v1beta1.EngineSpec{})
		assert.NoError(t, err)
		assert.Nil(t, merged.TopologyKey, "unset on both runtime and ISVC must stay nil")
	})
	t.Run("decoder-isvc-overrides-runtime", func(t *testing.T) {
		runtime := &v1beta1.DecoderSpec{TopologyKey: strPtr("runtime.domain/key")}
		isvc := &v1beta1.DecoderSpec{TopologyKey: strPtr("isvc.domain/key")}
		merged, err := MergeDecoderSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologyKey)
		assert.Equal(t, "isvc.domain/key", *merged.TopologyKey)
	})
	t.Run("decoder-inherits-runtime-when-isvc-unset", func(t *testing.T) {
		runtime := &v1beta1.DecoderSpec{TopologyKey: strPtr("topology.example.com/domain")}
		isvc := &v1beta1.DecoderSpec{}
		merged, err := MergeDecoderSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologyKey)
		assert.Equal(t, "topology.example.com/domain", *merged.TopologyKey)
	})
}

func TestMergeDecoderSpec(t *testing.T) {
	intPtr := func(i int) *int { return &i }
	int64Ptr := func(i int64) *int64 { return &i }

	tests := []struct {
		name            string
		runtimeDecoder  *v1beta1.DecoderSpec
		isvcDecoder     *v1beta1.DecoderSpec
		expectedDecoder *v1beta1.DecoderSpec
		expectError     bool
	}{
		{
			name:            "nil inputs",
			runtimeDecoder:  nil,
			isvcDecoder:     nil,
			expectedDecoder: nil,
			expectError:     false,
		},
		{
			name: "isvc spec is nil, runtime spec is not nil",
			runtimeDecoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(2),
					MaxReplicas: 5,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "runtime:v1",
						},
					},
				},
			},
			isvcDecoder:     nil,
			expectedDecoder: nil,
			expectError:     false,
		},
		{
			name:           "runtime spec is nil, isvc spec is not nil",
			runtimeDecoder: nil,
			isvcDecoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 5,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "isvc:v1",
						},
					},
				},
			},
			expectedDecoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 5,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "isvc:v1",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "merge with leader/worker specs for multi-node decoder",
			runtimeDecoder: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "leader",
								Image: "runtime-leader:v1",
							},
						},
						NodeSelector: map[string]string{
							"node-role": "leader",
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker",
								Image: "runtime-worker:v1",
							},
						},
					},
				},
			},
			isvcDecoder: &v1beta1.DecoderSpec{
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(4),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker",
								Image: "isvc-worker:v2",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										"nvidia.com/gpu": resource.MustParse("1"),
									},
								},
							},
						},
					},
				},
			},
			expectedDecoder: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "leader",
								Image: "runtime-leader:v1",
							},
						},
						NodeSelector: map[string]string{
							"node-role": "leader",
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(4),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker",
								Image: "isvc-worker:v2",
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										"nvidia.com/gpu": resource.MustParse("1"),
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "complex PD-disaggregated decoder merge",
			runtimeDecoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:    intPtr(2),
					MaxReplicas:    8,
					TimeoutSeconds: int64Ptr(80),
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "pd-decoder:v1",
							Env: []v1.EnvVar{
								{Name: "KV_CACHE_SIZE", Value: "16GB"},
								{Name: "DECODE_BATCH_SIZE", Value: "32"},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "kv-cache",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
					},
				},
			},
			isvcDecoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MaxReplicas: 16,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Env: []v1.EnvVar{
								{Name: "DECODE_BATCH_SIZE", Value: "64"},
								{Name: "MAX_TOKENS", Value: "2048"},
							},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("2"),
									v1.ResourceMemory: resource.MustParse("2Gi"),
									"nvidia.com/gpu":  resource.MustParse("1"),
								},
								Limits: v1.ResourceList{
									"nvidia.com/gpu": resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedDecoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:    intPtr(2),
					MaxReplicas:    16,
					TimeoutSeconds: int64Ptr(80),
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "pd-decoder:v1",
							Env: []v1.EnvVar{
								{Name: "KV_CACHE_SIZE", Value: "16GB"},
								{Name: "DECODE_BATCH_SIZE", Value: "64"},
								{Name: "MAX_TOKENS", Value: "2048"},
							},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("2"),
									v1.ResourceMemory: resource.MustParse("2Gi"),
									"nvidia.com/gpu":  resource.MustParse("1"),
								},
								Limits: v1.ResourceList{
									"nvidia.com/gpu": resource.MustParse("1"),
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "kv-cache",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium: v1.StorageMediumMemory,
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "merge runner spec with nested container fields",
			runtimeDecoder: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:    "decoder-runner",
						Image:   "runtime-decoder:v1",
						Command: []string{"/bin/decode"},
						Args:    []string{"--mode", "streaming"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "models",
								MountPath: "/models",
							},
						},
					},
				},
			},
			isvcDecoder: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Args: []string{"--mode", "batch", "--batch-size", "64"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "cache",
								MountPath: "/cache",
							},
							{
								Name:      "models",
								MountPath: "/models",
							},
						},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"nvidia.com/gpu": resource.MustParse("1"),
							},
						},
					},
				},
			},
			expectedDecoder: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						// Name restored from runtime — ISVC RunnerSpec
						// omitted Name (kubectl-defaulted zero value
						// because the patch only set image-adjacent
						// fields), which would otherwise wipe the
						// runtime's name and produce a pod apiserver
						// rejects with `spec.containers[0].name:
						// Required value`. See restoreRunnerName in
						// merging.go.
						Name:    "decoder-runner",
						Image:   "runtime-decoder:v1",
						Command: []string{"/bin/decode"},
						Args:    []string{"--mode", "batch", "--batch-size", "64"},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "cache",
								MountPath: "/cache",
							},
							{
								Name:      "models",
								MountPath: "/models",
							},
						},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"nvidia.com/gpu": resource.MustParse("1"),
							},
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeDecoderSpec(tt.runtimeDecoder, tt.isvcDecoder)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedDecoder, result)
			}
		})
	}
}

func TestRoleDeploymentModeOfAnEngine(t *testing.T) {
	intPtr := func(i int) *int { return &i }

	tests := []struct {
		name         string
		engine       *v1beta1.EngineSpec
		expectedMode constants.DeploymentModeType
	}{
		{
			name:         "nil engine spec",
			engine:       nil,
			expectedMode: constants.RawDeployment,
		},
		{
			name: "multi-node with leader and worker",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{},
			},
			expectedMode: constants.OMENative,
		},
		{
			name: "multi-node with only leader",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
			},
			expectedMode: constants.OMENative,
		},
		{
			name: "multi-node with only worker",
			engine: &v1beta1.EngineSpec{
				Worker: &v1beta1.WorkerSpec{},
			},
			expectedMode: constants.OMENative,
		},
		{
			name: "raw deployment with min replicas > 0",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
				},
			},
			expectedMode: constants.RawDeployment,
		},
		{
			name: "raw deployment with only runner",
			engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "runner",
						Image: "runner:latest",
					},
				},
			},
			expectedMode: constants.RawDeployment,
		},
		{
			name:         "raw deployment with empty spec",
			engine:       &v1beta1.EngineSpec{},
			expectedMode: constants.RawDeployment,
		},
		{
			name: "annotation with MultiNode takes highest precedence",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{
						constants.DeploymentMode: string(constants.MultiNode),
					},
					MinReplicas: intPtr(0),
				},
			},
			expectedMode: constants.MultiNode,
		},
		{
			name: "invalid annotation is ignored, falls back to leader check",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{
						constants.DeploymentMode: "InvalidMode",
					},
					MinReplicas: intPtr(1),
				},
				Leader: &v1beta1.LeaderSpec{},
			},
			expectedMode: constants.OMENative,
		},
		{
			name: "empty annotations map, falls back to leader check",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{},
					MinReplicas: intPtr(1),
				},
				Leader: &v1beta1.LeaderSpec{},
			},
			expectedMode: constants.OMENative,
		},
		{
			name: "annotation overrides leader and min replicas 0",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{
						constants.DeploymentMode: string(constants.RawDeployment),
					},
					MinReplicas: intPtr(0),
				},
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{},
			},
			expectedMode: constants.RawDeployment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RoleDeploymentMode(v1beta1.RoleSpecs{Engine: tt.engine}.View(v1beta1.EngineComponent), nil)
			assert.Equal(t, tt.expectedMode, result)
		})
	}
}

func TestDetermineEntrypointComponent(t *testing.T) {
	tests := []struct {
		name               string
		isvc               *v1beta1.InferenceService
		expectedEntrypoint v1beta1.ComponentType
	}{
		{
			name: "engine only",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			expectedEntrypoint: v1beta1.EngineComponent,
		},
		{
			name: "engine + router - router takes precedence",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
					Router: &v1beta1.RouterSpec{},
				},
			},
			expectedEntrypoint: v1beta1.RouterComponent,
		},
		{
			name: "all components - router takes precedence",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{},
					Decoder: &v1beta1.DecoderSpec{},
					Router:  &v1beta1.RouterSpec{},
				},
			},
			expectedEntrypoint: v1beta1.RouterComponent,
		},
		{
			name: "engine + decoder - engine is entrypoint (no router)",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{},
					Decoder: &v1beta1.DecoderSpec{},
				},
			},
			expectedEntrypoint: v1beta1.EngineComponent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entrypoint := DetermineEntrypointComponent(tt.isvc)
			assert.Equal(t, tt.expectedEntrypoint, entrypoint)
		})
	}
}

func TestMergedRunnerPorts(t *testing.T) {
	enginePorts := []v1.ContainerPort{
		{Name: "http", ContainerPort: 8000},
		{Name: "dist", ContainerPort: 5000},
	}
	routerPorts := []v1.ContainerPort{{Name: "http", ContainerPort: 30080}}

	engine := &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Ports: enginePorts}}}
	router := &v1beta1.RouterSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Ports: routerPorts}}}
	// Decoder present but portless: omitted, so the caller sees "undeclared"
	// rather than an empty-but-present entry it might read as authoritative.
	decoder := &v1beta1.DecoderSpec{Runner: &v1beta1.RunnerSpec{}}

	got := MergedRunnerPorts(v1beta1.RoleSpecs{Engine: engine, Decoder: decoder, Router: router})
	assert.Equal(t, enginePorts, got[v1beta1.EngineComponent])
	assert.Equal(t, routerPorts, got[v1beta1.RouterComponent])
	assert.NotContains(t, got, v1beta1.DecoderComponent)

	// Absent Components and portless Components contribute nothing.
	assert.Empty(t, MergedRunnerPorts(v1beta1.RoleSpecs{}))
	assert.Empty(t, MergedRunnerPorts(v1beta1.RoleSpecs{Engine: &v1beta1.EngineSpec{}}))
}

// The serving container's ports can be declared on the Component's own pod
// container rather than on the runner; the pod renderer merges the two, so
// port resolution must follow the same path.
func TestMergedRunnerPorts_FallsBackToPodContainer(t *testing.T) {
	podPorts := []v1.ContainerPort{{Name: "http", ContainerPort: 8080}}

	t.Run("no runner uses the first container", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{PodSpec: v1beta1.PodSpec{
			Containers: []v1.Container{{Name: "ome-container", Ports: podPorts}},
		}}
		assert.Equal(t, podPorts, MergedRunnerPorts(v1beta1.RoleSpecs{Engine: engine})[v1beta1.EngineComponent])
	})

	t.Run("portless runner uses the container it merges into", func(t *testing.T) {
		engine := &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "runner"}},
			PodSpec: v1beta1.PodSpec{Containers: []v1.Container{
				{Name: "sidecar", Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 9999}}},
				{Name: "runner", Ports: podPorts},
			}},
		}
		assert.Equal(t, podPorts, MergedRunnerPorts(v1beta1.RoleSpecs{Engine: engine})[v1beta1.EngineComponent])
	})
}

func TestMergedRunnerPorts_UsesEffectiveLeaderTemplateForMultiPod(t *testing.T) {
	workerSize := 1
	cases := []struct {
		name      string
		engine    *v1beta1.EngineSpec
		decoder   *v1beta1.DecoderSpec
		component v1beta1.ComponentType
		want      []v1.ContainerPort
	}{
		{
			name: "Engine Leader runner",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 8100}},
				}}},
				Worker: &v1beta1.WorkerSpec{Size: &workerSize},
			},
			component: v1beta1.EngineComponent,
			want:      []v1.ContainerPort{{Name: "http", ContainerPort: 8100}},
		},
		{
			name: "Engine Leader pod container",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{Containers: []v1.Container{{
					Name: "engine", Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 8200}},
				}}}},
				Worker: &v1beta1.WorkerSpec{Size: &workerSize},
			},
			component: v1beta1.EngineComponent,
			want:      []v1.ContainerPort{{Name: "http", ContainerPort: 8200}},
		},
		{
			name: "Decoder Leader runner",
			decoder: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 8300}},
				}}},
				Worker: &v1beta1.WorkerSpec{Size: &workerSize},
			},
			component: v1beta1.DecoderComponent,
			want:      []v1.ContainerPort{{Name: "http", ContainerPort: 8300}},
		},
		{
			name: "Decoder Leader pod container",
			decoder: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{Containers: []v1.Container{{
					Name: "decoder", Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 8400}},
				}}}},
				Worker: &v1beta1.WorkerSpec{Size: &workerSize},
			},
			component: v1beta1.DecoderComponent,
			want:      []v1.ContainerPort{{Name: "http", ContainerPort: 8400}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				engine  = tc.engine
				decoder = tc.decoder
				err     error
			)
			if engine != nil {
				engine, err = MergeEngineSpec(engine, &v1beta1.EngineSpec{})
				if err != nil {
					t.Fatalf("MergeEngineSpec: %v", err)
				}
			}
			if decoder != nil {
				decoder, err = MergeDecoderSpec(decoder, &v1beta1.DecoderSpec{})
				if err != nil {
					t.Fatalf("MergeDecoderSpec: %v", err)
				}
			}
			assert.Equal(t, tc.want, MergedRunnerPorts(v1beta1.RoleSpecs{Engine: engine, Decoder: decoder})[tc.component])
		})
	}
}

func TestMergedRunnerPorts_InvalidMultiPodShapeUsesTopLevelTemplate(t *testing.T) {
	topLevelPorts := []v1.ContainerPort{{Name: "http", ContainerPort: 8500}}
	leaderPorts := []v1.ContainerPort{{Name: "http", ContainerPort: 8600}}
	zeroWorkers := 0

	engine := &v1beta1.EngineSpec{
		Runner: &v1beta1.RunnerSpec{Container: v1.Container{Ports: topLevelPorts}},
		Leader: &v1beta1.LeaderSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Ports: leaderPorts}}},
		Worker: &v1beta1.WorkerSpec{Size: &zeroWorkers},
	}
	decoder := &v1beta1.DecoderSpec{
		Runner: &v1beta1.RunnerSpec{Container: v1.Container{Ports: topLevelPorts}},
		Leader: &v1beta1.LeaderSpec{Runner: &v1beta1.RunnerSpec{Container: v1.Container{Ports: leaderPorts}}},
	}

	got := MergedRunnerPorts(v1beta1.RoleSpecs{Engine: engine, Decoder: decoder})
	assert.Equal(t, topLevelPorts, got[v1beta1.EngineComponent])
	assert.Equal(t, topLevelPorts, got[v1beta1.DecoderComponent])
}

func TestGetTargetServicePort(t *testing.T) {
	tests := []struct {
		name         string
		isvc         *v1beta1.InferenceService
		services     []v1.Service
		expectedPort int32
		expectError  bool
	}{
		{
			name: "raw deployment mode - engine only with custom port",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			services: []v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-isvc-engine",
						Namespace: "default",
					},
					Spec: v1.ServiceSpec{
						Ports: []v1.ServicePort{
							{Port: 8081},
						},
					},
				},
			},
			expectedPort: 8081,
			expectError:  false,
		},
		{
			name: "raw deployment mode - with router and custom port",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
					Router: &v1beta1.RouterSpec{},
				},
			},
			services: []v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-isvc-router",
						Namespace: "default",
					},
					Spec: v1.ServiceSpec{
						Ports: []v1.ServicePort{
							{Port: 8082},
						},
					},
				},
			},
			expectedPort: 8082,
			expectError:  false,
		},
		{
			name: "raw deployment mode - service not found",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			services:     []v1.Service{},
			expectedPort: 0,
			expectError:  true,
		},
		{
			name: "raw deployment mode - service with no ports uses default",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			services: []v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-isvc-engine",
						Namespace: "default",
					},
					Spec: v1.ServiceSpec{
						Ports: []v1.ServicePort{},
					},
				},
			},
			expectedPort: constants.CommonISVCPort,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build fake client with services
			objs := make([]client.Object, 0, len(tt.services))
			for i := range tt.services {
				objs = append(objs, &tt.services[i])
			}
			fakeClient := fakeclient.New(t, fakeclient.WithObjects(objs...))

			port, err := GetTargetServicePort(context.Background(), fakeClient, tt.isvc)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPort, port)
			}
		})
	}
}

func TestGetTargetServicePort_ServiceNameResolution(t *testing.T) {
	// Test that the correct service names are used based on mode and router presence
	tests := []struct {
		name                string
		isvc                *v1beta1.InferenceService
		expectedServiceName string
	}{
		{
			name: "raw mode - engine only uses EngineServiceName",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-model",
					Namespace: "test-ns",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			expectedServiceName: constants.EngineServiceName("my-model"), // my-model-engine
		},
		{
			name: "raw mode - with router uses RouterServiceName",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-model",
					Namespace: "test-ns",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
					Router: &v1beta1.RouterSpec{},
				},
			},
			expectedServiceName: constants.RouterServiceName("my-model"), // my-model-router
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a service with the expected name
			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.expectedServiceName,
					Namespace: tt.isvc.Namespace,
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{Port: 9999}, // Use distinct port to verify correct service was found
					},
				},
			}

			fakeClient := fakeclient.New(t, fakeclient.WithObjects(svc))

			port, err := GetTargetServicePort(context.Background(), fakeClient, tt.isvc)

			assert.NoError(t, err)
			assert.Equal(t, int32(9999), port, "Should find the service with expected name: %s", tt.expectedServiceName)
		})
	}
}

func TestAddNodeSelectorForReadyModel(t *testing.T) {
	tests := []struct {
		name             string
		podSpec          *v1.PodSpec
		baseModelMeta    *metav1.ObjectMeta
		wantNodeSelector bool
		wantLabelKey     string
	}{
		{
			name:    "ClusterBaseModel - adds node selector",
			podSpec: &v1.PodSpec{},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "test-cluster-model",
				Namespace: "", // Empty namespace indicates ClusterBaseModel
			},
			wantNodeSelector: true,
			wantLabelKey:     "models.ome.io/clusterbasemodel.test-cluster-model",
		},
		{
			name:    "BaseModel (namespace-scoped) - adds node selector",
			podSpec: &v1.PodSpec{},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "test-model",
				Namespace: "default",
			},
			wantNodeSelector: true,
			wantLabelKey:     "models.ome.io/default.basemodel.test-model",
		},
		{
			name:             "nil podSpec - no panic",
			podSpec:          nil,
			baseModelMeta:    &metav1.ObjectMeta{Name: "test-model", Namespace: "default"},
			wantNodeSelector: false,
		},
		{
			name:             "nil baseModelMeta - no panic",
			podSpec:          &v1.PodSpec{},
			baseModelMeta:    nil,
			wantNodeSelector: false,
		},
		{
			name: "existing node selector - adds without overwriting",
			podSpec: &v1.PodSpec{
				NodeSelector: map[string]string{
					"existing-label": "value",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "test-model",
				Namespace: "default",
			},
			wantNodeSelector: true,
			wantLabelKey:     "models.ome.io/default.basemodel.test-model",
		},
		{
			name: "duplicate label check - does not overwrite existing",
			podSpec: &v1.PodSpec{
				NodeSelector: map[string]string{
					"models.ome.io/default.basemodel.test-model": "Ready",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "test-model",
				Namespace: "default",
			},
			wantNodeSelector: true,
			wantLabelKey:     "models.ome.io/default.basemodel.test-model",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialLabelCount := 0
			if tt.podSpec != nil && tt.podSpec.NodeSelector != nil {
				initialLabelCount = len(tt.podSpec.NodeSelector)
			}

			AddNodeSelectorForModelReadyNode(tt.podSpec, tt.baseModelMeta)

			if !tt.wantNodeSelector {
				// For nil cases, just verify no panic occurred
				return
			}

			assert.NotNil(t, tt.podSpec.NodeSelector, "NodeSelector should not be nil")

			// Check that the expected label key exists with value "Ready"
			value, found := tt.podSpec.NodeSelector[tt.wantLabelKey]
			assert.True(t, found, "Expected label key %s not found in node selector", tt.wantLabelKey)
			assert.Equal(t, "Ready", value, "Label value should be 'Ready'")

			// For the duplicate test case, verify no additional label was added
			if tt.name == "duplicate label check - does not overwrite existing" {
				assert.Equal(t, initialLabelCount, len(tt.podSpec.NodeSelector),
					"Should not add duplicate node selector label")
			}

			// For the existing node selector case, verify the existing label is preserved
			if tt.name == "existing node selector - adds without overwriting" {
				assert.Equal(t, initialLabelCount+1, len(tt.podSpec.NodeSelector),
					"Should add new node selector label")
				existingValue, existingFound := tt.podSpec.NodeSelector["existing-label"]
				assert.True(t, existingFound, "Existing node selector label should be preserved")
				assert.Equal(t, "value", existingValue)
			}
		})
	}
}

// TestMergeTopologySpread_Resolution pins the effective-topologySpread rule:
// the ISVC component value wins when set, otherwise the runtime
// component-config value is inherited; unset on both stays nil (opt-in,
// zero behavior change — placement remains pure bin-packing).
func TestMergeTopologySpread_Resolution(t *testing.T) {
	spread := func(p v1beta1.TopologySpreadPolicy) *v1beta1.TopologySpreadPolicy { return &p }

	t.Run("engine-isvc-overrides-runtime", func(t *testing.T) {
		runtime := &v1beta1.EngineSpec{TopologySpread: spread(v1beta1.TopologySpreadPreferred)}
		isvc := &v1beta1.EngineSpec{TopologySpread: spread(v1beta1.TopologySpreadRequired)}
		merged, err := MergeEngineSpec(runtime, isvc)
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologySpread)
		assert.Equal(t, v1beta1.TopologySpreadRequired, *merged.TopologySpread, "ISVC topologySpread must override the runtime value")
	})
	t.Run("engine-inherits-runtime-when-isvc-unset", func(t *testing.T) {
		runtime := &v1beta1.EngineSpec{TopologySpread: spread(v1beta1.TopologySpreadPreferred)}
		merged, err := MergeEngineSpec(runtime, &v1beta1.EngineSpec{})
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologySpread, "runtime topologySpread must be inherited when ISVC leaves it unset")
		assert.Equal(t, v1beta1.TopologySpreadPreferred, *merged.TopologySpread)
	})
	t.Run("engine-nil-on-both", func(t *testing.T) {
		merged, err := MergeEngineSpec(&v1beta1.EngineSpec{}, &v1beta1.EngineSpec{})
		assert.NoError(t, err)
		assert.Nil(t, merged.TopologySpread, "unset on both runtime and ISVC must stay nil")
	})
	t.Run("decoder-inherits-runtime-when-isvc-unset", func(t *testing.T) {
		runtime := &v1beta1.DecoderSpec{TopologySpread: spread(v1beta1.TopologySpreadRequired)}
		merged, err := MergeDecoderSpec(runtime, &v1beta1.DecoderSpec{})
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologySpread)
		assert.Equal(t, v1beta1.TopologySpreadRequired, *merged.TopologySpread)
	})
	t.Run("engine-spread-key-follows-the-same-rule", func(t *testing.T) {
		key := func(s string) *string { return &s }
		runtime := &v1beta1.EngineSpec{TopologySpreadKey: key("cloud.google.com/gce-topology-subblock")}
		merged, err := MergeEngineSpec(runtime, &v1beta1.EngineSpec{})
		assert.NoError(t, err)
		assert.NotNil(t, merged.TopologySpreadKey, "runtime topologySpreadKey must be inherited when ISVC leaves it unset")
		assert.Equal(t, "cloud.google.com/gce-topology-subblock", *merged.TopologySpreadKey)
	})
}
