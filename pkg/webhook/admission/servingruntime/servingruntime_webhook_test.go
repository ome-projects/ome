package servingruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

func TestValidateServingRuntimePriority(t *testing.T) {
	scenarios := map[string]struct {
		name                   string
		newServingRuntime      *v1beta1.ServingRuntime
		existingServingRuntime *v1beta1.ServingRuntime
		expected               gomega.OmegaMatcher
	}{
		"When existing serving runtime is disabled it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "new-runtime",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-runtime",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},
					Disabled: proto.Bool(true),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When new serving runtime and existing runtime are same it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When model format is different it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "lightgbm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When autoselect is false in the new serving runtime it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(false),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When autoselect is not specified in the new serving runtime it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:     "vllm",
							Version:  proto.String("1"),
							Priority: proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When autoselect is false in the existing serving runtime it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(false),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When model version is nil in both serving runtime and priority is same then it should return error": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.Equal(fmt.Errorf(InvalidPriorityError, "vllm")),
		},
		"When model version is nil in both serving runtime and priority is not same then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(2),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When model version is nil in new serving runtime and priority is same then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When model version is nil in existing serving runtime and priority is same then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When two serving runtime has the same supported model format then it should return error": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "vllm",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("CohereForCausalLM"),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "vllm",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("CohereForCausalLM"),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.Equal(fmt.Errorf(InvalidPriorityError, "vllm")),
		},
		"When two serving runtime has the same supported model format but different architecture then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "vllm",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("CohereForCausalLM"),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "vllm",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("Cohere2ForCasualLM"),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When two serving runtime has the same supported model format but different framework then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "vllm",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("LlamaForCasualLM"),
							ModelFormat:       &v1beta1.ModelFormat{Name: "safetensors", Version: proto.String("1.0.0")},
							ModelFramework:    &v1beta1.ModelFrameworkSpec{Name: "Transformers", Version: proto.String("1.0.0")},
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "vllm",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("LlamaForCasualLM"),
							ModelFormat:       &v1beta1.ModelFormat{Name: "safetensors", Version: proto.String("1.0.0")},
							ModelFramework:    &v1beta1.ModelFrameworkSpec{Name: "Transformers", Version: proto.String("1.2.0")},
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When two serving runtime has the same supported model format but different size then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "safetensors",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("LlamaForCasualLM"),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
						Min: proto.String("100B"),
						Max: proto.String("200B"),
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:              "safetensors",
							Version:           proto.String("1"),
							AutoSelect:        proto.Bool(true),
							Priority:          proto.Int32(1),
							ModelArchitecture: proto.String("LlamaForCasualLM"),
						},
					},
					Disabled: proto.Bool(false),
					ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
						Min: proto.String("300B"),
						Max: proto.String("600B"),
					},
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When model version is same in both serving runtime and priority is same then it should return error": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.Equal(fmt.Errorf(InvalidPriorityError, "vllm")),
		},
		"When model version is different but priority is same then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1.3"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1.0"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When priority is nil in both serving runtime then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When priority is nil in new serving runtime and priority is specified in existing serving runtime then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"When priority is nil in existing serving runtime and priority is specified in new serving runtime then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			existingServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-2",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							Version:    proto.String("1"),
							AutoSelect: proto.Bool(true),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "seldonio/mlserver:1.2.0",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			err := validateServingRuntimePriority(&scenario.newServingRuntime.Spec, &scenario.existingServingRuntime.Spec,
				scenario.newServingRuntime.Name, scenario.existingServingRuntime.Name)
			g.Expect(err).To(scenario.expected)
		})
	}
}

func TestValidateServingRuntimeAnnotations(t *testing.T) {
	scenarios := map[string]struct {
		spec    v1beta1.ServingRuntimeSpec
		matcher gomega.OmegaMatcher
	}{
		"When chainsaw inject annotation is not set then it should return nil": {
			spec: v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{},
			},
			matcher: gomega.BeNil(),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			err := validateServingRuntimeAnnotations(&scenario.spec)
			g.Expect(err).To(scenario.matcher)
		})
	}
}

func TestValidateModelFormatPrioritySame(t *testing.T) {
	scenarios := map[string]struct {
		newServingRuntime *v1beta1.ServingRuntime
		expected          gomega.OmegaMatcher
	}{
		"When different priority assigned for the same model format in the runtime then it should return error": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(1),
						},
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(2),
						},
					},

					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.Equal(fmt.Errorf("different priorities assigned for the model format %s", "vllm")),
		},
		"When same priority assigned for the same model format in the runtime then it should return nil": {
			newServingRuntime: &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-runtime-1",
					Namespace: "test",
				},
				Spec: v1beta1.ServingRuntimeSpec{
					SupportedModelFormats: []v1beta1.SupportedModelFormat{
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(2),
						},
						{
							Name:       "vllm",
							AutoSelect: proto.Bool(true),
							Priority:   proto.Int32(2),
						},
					},
					Disabled: proto.Bool(false),
					ProtocolVersions: []constants.InferenceServiceProtocol{
						constants.OpenAIProtocol,
						constants.OpenAIProtocol,
					},
					ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
						Containers: []corev1.Container{
							{
								Name:  constants.MainContainerName,
								Image: "ome/vllm:latest",
								Args: []string{
									"--model_name={{.Name}}",
									"--model_dir=/mnt/models",
									"--http_port=8080",
								},
							},
						},
					},
				},
			},
			expected: gomega.BeNil(),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			err := validation.ValidateModelFormatPrioritySame(&scenario.newServingRuntime.Spec)
			g.Expect(err).To(scenario.expected)
		})
	}
}

func TestAreModelSizeRangesEqual(t *testing.T) {
	testcases := []struct {
		name     string
		range1   *v1beta1.ModelSizeRangeSpec
		range2   *v1beta1.ModelSizeRangeSpec
		expected bool
	}{
		{
			name:     "Both nil",
			range1:   nil,
			range2:   nil,
			expected: true,
		},
		{
			name:     "First nil, second not nil",
			range1:   nil,
			range2:   &v1beta1.ModelSizeRangeSpec{},
			expected: false,
		},
		{
			name:     "First not nil, second nil",
			range1:   &v1beta1.ModelSizeRangeSpec{},
			range2:   nil,
			expected: false,
		},
		{
			name:     "Both empty",
			range1:   &v1beta1.ModelSizeRangeSpec{},
			range2:   &v1beta1.ModelSizeRangeSpec{},
			expected: true,
		},
		{
			name: "Different Min values",
			range1: &v1beta1.ModelSizeRangeSpec{
				Min: stringPointer("10"),
			},
			range2: &v1beta1.ModelSizeRangeSpec{
				Min: stringPointer("20"),
			},
			expected: false,
		},
		{
			name: "Different Max values",
			range1: &v1beta1.ModelSizeRangeSpec{
				Max: stringPointer("100"),
			},
			range2: &v1beta1.ModelSizeRangeSpec{
				Max: stringPointer("200"),
			},
			expected: false,
		},
		{
			name:   "First Min nil, second Min not nil",
			range1: &v1beta1.ModelSizeRangeSpec{},
			range2: &v1beta1.ModelSizeRangeSpec{
				Min: stringPointer("10"),
			},
			expected: false,
		},
		{
			name:   "First Max nil, second Max not nil",
			range1: &v1beta1.ModelSizeRangeSpec{},
			range2: &v1beta1.ModelSizeRangeSpec{
				Max: stringPointer("100"),
			},
			expected: false,
		},
		{
			name: "Equal Min and Max values",
			range1: &v1beta1.ModelSizeRangeSpec{
				Min: stringPointer("10"),
				Max: stringPointer("100"),
			},
			range2: &v1beta1.ModelSizeRangeSpec{
				Min: stringPointer("10"),
				Max: stringPointer("100"),
			},
			expected: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := areModelSizeRangesEqual(tc.range1, tc.range2)
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}

// TestSRValidateAnnotations tests the validateServingRuntimeAnnotations function
func TestSRValidateAnnotations(t *testing.T) {
	testcases := []struct {
		name        string
		annotations map[string]string
		expected    gomega.OmegaMatcher
	}{
		{
			name:        "No annotations",
			annotations: nil,
			expected:    gomega.BeNil(),
		},
		{
			name:        "Empty annotations",
			annotations: map[string]string{},
			expected:    gomega.BeNil(),
		},
		{
			name: "Valid annotations",
			annotations: map[string]string{
				"some-key": "some-value",
			},
			expected: gomega.BeNil(),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)

			servingRuntime := &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Annotations: tc.annotations,
				},
			}

			err := validateServingRuntimeAnnotations(servingRuntime)
			g.Expect(err).To(tc.expected)
		})
	}
}

// TestContains tests the contains function
func TestContains(t *testing.T) {
	testcases := []struct {
		name     string
		slice    []string
		element  string
		expected bool
	}{
		{
			name:     "Empty slice",
			slice:    []string{},
			element:  "test",
			expected: false,
		},
		{
			name:     "Element not in slice",
			slice:    []string{"a", "b", "c"},
			element:  "d",
			expected: false,
		},
		{
			name:     "Element in slice",
			slice:    []string{"a", "b", "c"},
			element:  "b",
			expected: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := contains(tc.slice, tc.element)
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}

// TestValidateServingRuntimeAnnotationsComplete tests edge cases of validateServingRuntimeAnnotations
func TestValidateServingRuntimeAnnotationsComplete(t *testing.T) {
	testcases := []struct {
		name        string
		annotations map[string]string
		expected    gomega.OmegaMatcher
	}{
		{
			name:        "No annotations",
			annotations: nil,
			expected:    gomega.BeNil(),
		},
		{
			name:        "Empty annotations",
			annotations: map[string]string{},
			expected:    gomega.BeNil(),
		},
		{
			name: "Valid annotations",
			annotations: map[string]string{
				"some-key": "some-value",
			},
			expected: gomega.BeNil(),
		},
		{
			name: "Chainsaw annotation - not implemented yet",
			annotations: map[string]string{
				"chainsaw.inject": "true",
			},
			expected: gomega.BeNil(), // Currently this returns nil since the functionality is not implemented
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)

			servingRuntime := &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Annotations: tc.annotations,
				},
			}

			err := validateServingRuntimeAnnotations(servingRuntime)
			g.Expect(err).To(tc.expected)
		})
	}
}

// TestContainsComplete tests the contains function thoroughly
func TestContainsComplete(t *testing.T) {
	testcases := []struct {
		name     string
		slice    []string
		element  string
		expected bool
	}{
		{
			name:     "Empty slice",
			slice:    []string{},
			element:  "test",
			expected: false,
		},
		{
			name:     "Element not in slice",
			slice:    []string{"a", "b", "c"},
			element:  "d",
			expected: false,
		},
		{
			name:     "Element in slice",
			slice:    []string{"a", "b", "c"},
			element:  "b",
			expected: true,
		},
		{
			name:     "Edge case - nil slice",
			slice:    nil,
			element:  "test",
			expected: false,
		},
		{
			name:     "Edge case - empty string",
			slice:    []string{"a", "", "c"},
			element:  "",
			expected: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := contains(tc.slice, tc.element)
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}

// StringPointer returns a pointer to the given string value
func stringPointer(s string) *string {
	return &s
}

// mkCSRWithFormat builds a ClusterServingRuntime that auto-selects on
// `formatName` at the given priority. The pod spec is the minimal valid
// shape required to pass the webhook's structural checks.
func mkCSRWithFormat(name, formatName string, priority int32) *v1beta1.ClusterServingRuntime {
	return &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					Name:       formatName,
					Version:    proto.String("1"),
					AutoSelect: proto.Bool(true),
					Priority:   proto.Int32(priority),
				},
			},
			Disabled: proto.Bool(false),
			ProtocolVersions: []constants.InferenceServiceProtocol{
				constants.OpenAIProtocol,
			},
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				Containers: []corev1.Container{
					{
						Name:  constants.MainContainerName,
						Image: "ome/vllm:latest",
						Args:  []string{"--model_name={{.Name}}"},
					},
				},
			},
		},
	}
}

func newCSRDecoder(t *testing.T) admission.Decoder {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return admission.NewDecoder(s)
}

func encodeCSR(t *testing.T, csr *v1beta1.ClusterServingRuntime) []byte {
	t.Helper()
	raw, err := json.Marshal(csr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// TestClusterServingRuntimeValidator_PriorityConflictAcrossCSRs pins that
// the CSR validator compares the admitted CSR against every OTHER CSR:
// validateServingRuntimePriority receives the other CSR's name as
// `existingRuntimeName`, so the self-skip early-exit fires only for the
// CSR itself and cross-CSR priority conflicts are detected. Passing the
// admitted CSR's own name as both `existingRuntimeName` and
// `newRuntimeName` would make the early-exit fire for every pairing and
// let conflicts slip through.
//
// Scenario: two existing CSRs both auto-select `safetensors` at
// priorities 1 and 2. A third CSR is created auto-selecting
// `safetensors` at priority 1 (CONFLICTS with the first existing CSR)
// and must be rejected.
func TestClusterServingRuntimeValidator_PriorityConflictAcrossCSRs(t *testing.T) {
	g := gomega.NewWithT(t)

	existing1 := mkCSRWithFormat("rt-a", "safetensors", 1)
	existing2 := mkCSRWithFormat("rt-b", "safetensors", 2)

	c := ctrlclientfake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(existing1, existing2).
		Build()

	validator := &ClusterServingRuntimeValidator{Client: c, Decoder: newCSRDecoder(t)}

	// New CSR conflicts with rt-a (both priority 1 on `safetensors`).
	newCSR := mkCSRWithFormat("rt-c", "safetensors", 1)
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: encodeCSR(t, newCSR)},
		},
	}

	resp := validator.Handle(context.Background(), req)

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"expected priority conflict with rt-a to be REJECTED, got allowed=true")
	g.Expect(resp.Result).NotTo(gomega.BeNil())
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("safetensors"),
		"rejection should mention the conflicting model format")
}

// TestClusterServingRuntimeValidator_NoPriorityConflict confirms that
// a CSR with a non-conflicting priority on the same format is
// accepted, and (critically) that the fix does NOT over-reject by
// turning legitimate cross-CSR pairings into false positives.
func TestClusterServingRuntimeValidator_NoPriorityConflict(t *testing.T) {
	g := gomega.NewWithT(t)

	existing1 := mkCSRWithFormat("rt-a", "safetensors", 1)
	existing2 := mkCSRWithFormat("rt-b", "safetensors", 2)

	c := ctrlclientfake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(existing1, existing2).
		Build()

	validator := &ClusterServingRuntimeValidator{Client: c, Decoder: newCSRDecoder(t)}

	// Priority 3 on `safetensors` — non-conflicting with either existing.
	newCSR := mkCSRWithFormat("rt-c", "safetensors", 3)
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: encodeCSR(t, newCSR)},
		},
	}

	resp := validator.Handle(context.Background(), req)
	g.Expect(resp.Allowed).To(gomega.BeTrue(),
		"expected non-conflicting priority to be admitted, got rejection: %v", resp.Result)
}

// TestClusterServingRuntimeValidator_SelfUpdateNotFlagged confirms
// the fix preserves self-update behavior: when an existing CSR is
// updated, the loop iteration that hits the CSR's OWN current state
// (same name) is still skipped by the `existingRuntimeName ==
// newRuntimeName` early-exit, so a CSR can update its own spec
// without being flagged as a self-conflict.
func TestClusterServingRuntimeValidator_SelfUpdateNotFlagged(t *testing.T) {
	g := gomega.NewWithT(t)

	// `rt-a` already exists at priority 1; we are updating its spec
	// (still priority 1 on the same format). With the fix, the loop
	// iteration that hits `rt-a` itself should skip via the early-exit
	// because `existing.Items[i].Name == clusterServingRuntime.Name`.
	original := mkCSRWithFormat("rt-a", "safetensors", 1)
	c := ctrlclientfake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(original).
		Build()

	validator := &ClusterServingRuntimeValidator{Client: c, Decoder: newCSRDecoder(t)}

	// Same name, same format, same priority → self-update.
	updated := mkCSRWithFormat("rt-a", "safetensors", 1)
	// Tweak something benign so it's an actual update payload (the
	// validator doesn't distinguish create vs update for priority, but
	// this keeps the scenario realistic).
	updated.Spec.ServingRuntimePodSpec.Containers[0].Image = "ome/vllm:newer"
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Update,
			Object:    runtime.RawExtension{Raw: encodeCSR(t, updated)},
		},
	}

	resp := validator.Handle(context.Background(), req)
	g.Expect(resp.Allowed).To(gomega.BeTrue(),
		"self-update should not be flagged as a priority conflict; got rejection: %v", resp.Result)
}

// TestClusterServingRuntimeValidator_AutoscalerShape covers
// per-Component Autoscaler block validation on the CSR webhook path.
// Same shared validator.ValidateComponentAutoscaler that the ISVC
// webhook calls is invoked here against EngineConfig / DecoderConfig /
// RouterConfig so a malformed runtime-level default is rejected at
// admission rather than silently inherited into an ISVC at runtime.
func TestClusterServingRuntimeValidator_AutoscalerShape(t *testing.T) {
	mkCSRWithEngineAutoscaler := func(name string, as *v1beta1.ComponentAutoscaler) *v1beta1.ClusterServingRuntime {
		csr := mkCSRWithFormat(name, "safetensors", 1)
		csr.Spec.EngineConfig = &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: as},
		}
		return csr
	}

	tests := []struct {
		name        string
		csr         *v1beta1.ClusterServingRuntime
		wantAllowed bool
		wantSubstr  string
	}{
		{
			name:        "valid engineConfig.autoscaler hpa default",
			csr:         mkCSRWithEngineAutoscaler("rt-hpa", &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}),
			wantAllowed: true,
		},
		{
			name: "engineConfig.autoscaler keda with empty Triggers → denied",
			csr: mkCSRWithEngineAutoscaler("rt-bad-keda", &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda:  &v1beta1.KedaAutoscaler{},
			}),
			wantAllowed: false,
			wantSubstr:  "engineConfig",
		},
		{
			name: "engineConfig.autoscaler unknown class → denied",
			csr: mkCSRWithEngineAutoscaler("rt-bad-class", &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerClass("knative"),
			}),
			wantAllowed: false,
			wantSubstr:  "is not one of HPA|KEDA|External|None",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			c := ctrlclientfake.NewClientBuilder().WithScheme(newScheme(t)).Build()
			validator := &ClusterServingRuntimeValidator{Client: c, Decoder: newCSRDecoder(t)}

			req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Object:    runtime.RawExtension{Raw: encodeCSR(t, tc.csr)},
			}}
			resp := validator.Handle(context.Background(), req)

			g.Expect(resp.Allowed).To(gomega.Equal(tc.wantAllowed),
				"unexpected admission decision; resp=%v", resp.Result)
			if tc.wantSubstr != "" {
				g.Expect(resp.Result).NotTo(gomega.BeNil())
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(tc.wantSubstr))
			}
		})
	}
}

// TestValidator_SpecOnlyChecksRunWithNoExistingRuntimes verifies that
// the spec-only model-format priority check is enforced even when no
// other runtime exists: it runs independently of the loop over existing
// runtimes, so the first runtime admitted cannot escape it.
func TestValidator_SpecOnlyChecksRunWithNoExistingRuntimes(t *testing.T) {
	// Same auto-selected format name at two different priorities —
	// invalid per ValidateModelFormatPrioritySame.
	conflictingFormats := []v1beta1.SupportedModelFormat{
		{
			Name:       "safetensors",
			AutoSelect: proto.Bool(true),
			Priority:   proto.Int32(1),
		},
		{
			Name:       "safetensors",
			AutoSelect: proto.Bool(true),
			Priority:   proto.Int32(2),
		},
	}

	t.Run("clusterservingruntime", func(t *testing.T) {
		g := gomega.NewWithT(t)
		csr := mkCSRWithFormat("rt-solo", "safetensors", 1)
		csr.Spec.SupportedModelFormats = conflictingFormats

		c := ctrlclientfake.NewClientBuilder().WithScheme(newScheme(t)).Build()
		validator := &ClusterServingRuntimeValidator{Client: c, Decoder: newCSRDecoder(t)}

		req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: encodeCSR(t, csr)},
		}}
		resp := validator.Handle(context.Background(), req)

		g.Expect(resp.Allowed).To(gomega.BeFalse(),
			"conflicting per-format priorities must be rejected even with no existing CSRs")
		g.Expect(resp.Result.Message).To(gomega.ContainSubstring("safetensors"))
	})

	t.Run("servingruntime", func(t *testing.T) {
		g := gomega.NewWithT(t)
		csr := mkCSRWithFormat("rt-solo", "safetensors", 1)
		sr := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: "rt-solo", Namespace: "test"},
			Spec:       csr.Spec,
		}
		sr.Spec.SupportedModelFormats = conflictingFormats

		c := ctrlclientfake.NewClientBuilder().WithScheme(newScheme(t)).Build()
		validator := &ServingRuntimeValidator{Client: c, Decoder: newCSRDecoder(t)}

		raw, err := json.Marshal(sr)
		g.Expect(err).To(gomega.BeNil())
		req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: raw},
		}}
		resp := validator.Handle(context.Background(), req)

		g.Expect(resp.Allowed).To(gomega.BeFalse(),
			"conflicting per-format priorities must be rejected even with no existing SRs in the namespace")
		g.Expect(resp.Result.Message).To(gomega.ContainSubstring("safetensors"))
	})
}

// TestValidateAcceleratorClasses tests the validateAcceleratorClasses function
func TestValidateAcceleratorClasses(t *testing.T) {
	// Create fake client with pre-populated AcceleratorClasses
	existingClasses := []client.Object{
		&v1beta1.AcceleratorClass{
			ObjectMeta: metav1.ObjectMeta{Name: "nvidia-h100-80gb"},
		},
		&v1beta1.AcceleratorClass{
			ObjectMeta: metav1.ObjectMeta{Name: "nvidia-a100-80gb"},
		},
	}

	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	fakeClient := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingClasses...).
		Build()

	testcases := []struct {
		name        string
		spec        *v1beta1.ServingRuntimeSpec
		expectError bool
		errorMsg    string
	}{
		{
			name: "No accelerator requirements",
			spec: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: nil,
			},
			expectError: false,
		},
		{
			name: "Empty accelerator classes list",
			spec: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{},
				},
			},
			expectError: false,
		},
		{
			name: "Valid accelerator class - nvidia-h100-80gb",
			spec: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-h100-80gb"},
				},
			},
			expectError: false,
		},
		{
			name: "Unknown accelerator class",
			spec: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"unknown-accelerator"},
				},
			},
			expectError: true,
			errorMsg:    "unknown accelerator classes",
		},
		{
			name: "Multiple accelerator classes - all valid",
			spec: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-h100-80gb", "nvidia-a100-80gb"},
				},
			},
			expectError: false,
		},
		{
			name: "Multiple accelerator classes - one invalid",
			spec: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-h100-80gb", "invalid-class"},
				},
			},
			expectError: true,
			errorMsg:    "unknown accelerator classes",
		},
		{
			name: "Multiple accelerator classes - all invalid",
			spec: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"invalid-class-1", "invalid-class-2"},
				},
			},
			expectError: true,
			errorMsg:    "unknown accelerator classes",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			err := validateAcceleratorClasses(context.Background(), fakeClient, tc.spec)

			if tc.expectError {
				g.Expect(err).ToNot(gomega.BeNil())
				g.Expect(err.Error()).To(gomega.ContainSubstring(tc.errorMsg))
			} else {
				g.Expect(err).To(gomega.BeNil())
			}
		})
	}
}
