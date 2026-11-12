package runtimeselector

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// diffusionModel builds a BaseModelSpec whose format always matches the
// runtime fixture, so the diffusion-pipeline comparison is the only
// discriminating dimension in each case.
func diffusionModel(pipeline *v1beta1.DiffusionPipelineSpec) *v1beta1.BaseModelSpec {
	return &v1beta1.BaseModelSpec{
		ModelFormat:       v1beta1.ModelFormat{Name: "onnx"},
		DiffusionPipeline: pipeline,
	}
}

func diffusionRuntime(pipeline *v1beta1.DiffusionPipelineSpec) *v1beta1.ServingRuntimeSpec {
	return &v1beta1.ServingRuntimeSpec{
		SupportedModelFormats: []v1beta1.SupportedModelFormat{
			{
				Name:              "onnx",
				ModelFormat:       &v1beta1.ModelFormat{Name: "onnx"},
				DiffusionPipeline: pipeline,
			},
		},
	}
}

func TestDiffusionPipelineMatching(t *testing.T) {
	matcher := NewDefaultRuntimeMatcher(NewConfig(nil))
	isvc := &v1beta1.InferenceService{}

	tests := []struct {
		name           string
		model          *v1beta1.BaseModelSpec
		runtime        *v1beta1.ServingRuntimeSpec
		wantCompatible bool
		reasonContains string
	}{
		{
			name:           "runtime with no pipeline requirement accepts a diffusion model",
			model:          diffusionModel(&v1beta1.DiffusionPipelineSpec{ClassName: ptr("StableDiffusionXLPipeline")}),
			runtime:        diffusionRuntime(nil),
			wantCompatible: true,
		},
		{
			name:           "runtime requires a pipeline the model does not declare",
			model:          diffusionModel(nil),
			runtime:        diffusionRuntime(&v1beta1.DiffusionPipelineSpec{ClassName: ptr("StableDiffusionXLPipeline")}),
			wantCompatible: false,
			reasonContains: "diffusion pipeline required by runtime but not specified in model",
		},
		{
			name:           "matching class name",
			model:          diffusionModel(&v1beta1.DiffusionPipelineSpec{ClassName: ptr("QwenImagePipeline")}),
			runtime:        diffusionRuntime(&v1beta1.DiffusionPipelineSpec{ClassName: ptr("QwenImagePipeline")}),
			wantCompatible: true,
		},
		{
			name:           "class name mismatch",
			model:          diffusionModel(&v1beta1.DiffusionPipelineSpec{ClassName: ptr("QwenImagePipeline")}),
			runtime:        diffusionRuntime(&v1beta1.DiffusionPipelineSpec{ClassName: ptr("StableDiffusionXLPipeline")}),
			wantCompatible: false,
			reasonContains: "pipeline class mismatch",
		},
		{
			name:           "runtime requires a class the model leaves unset",
			model:          diffusionModel(&v1beta1.DiffusionPipelineSpec{}),
			runtime:        diffusionRuntime(&v1beta1.DiffusionPipelineSpec{ClassName: ptr("StableDiffusionXLPipeline")}),
			wantCompatible: false,
			reasonContains: "pipeline class mismatch (model=<nil>",
		},
		{
			name: "component library mismatch (vae)",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{
				VAE: &v1beta1.DiffusionComponentSpec{Library: "diffusers", Type: "AutoencoderKL"},
			}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				VAE: &v1beta1.DiffusionComponentSpec{Library: "transformers"},
			}),
			wantCompatible: false,
			reasonContains: "vae library mismatch",
		},
		{
			name: "component type mismatch (transformer)",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{
				Transformer: &v1beta1.DiffusionComponentSpec{Library: "diffusers", Type: "UNet2DConditionModel"},
			}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				Transformer: &v1beta1.DiffusionComponentSpec{Type: "FluxTransformer2DModel"},
			}),
			wantCompatible: false,
			reasonContains: "transformer type mismatch",
		},
		{
			name: "runtime requires a component the model omits (tokenizer)",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{
				Scheduler: &v1beta1.DiffusionComponentSpec{Library: "diffusers"},
			}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				Tokenizer: &v1beta1.DiffusionComponentSpec{Library: "transformers"},
			}),
			wantCompatible: false,
			reasonContains: "component tokenizer required by runtime but not specified in model",
		},
		{
			name: "component with empty runtime constraints matches any model component",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{
				Scheduler: &v1beta1.DiffusionComponentSpec{Library: "diffusers", Type: "FlowMatchEulerDiscreteScheduler"},
			}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				Scheduler: &v1beta1.DiffusionComponentSpec{},
			}),
			wantCompatible: true,
		},
		{
			name:  "runtime requires additional components the model lacks entirely",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				AdditionalComponents: map[string]v1beta1.DiffusionComponentSpec{
					"image_encoder": {Library: "transformers"},
				},
			}),
			wantCompatible: false,
			reasonContains: "diffusion pipeline missing required additional components",
		},
		{
			name: "runtime requires an additional component key missing in the model",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{
				AdditionalComponents: map[string]v1beta1.DiffusionComponentSpec{
					"feature_extractor": {Library: "transformers"},
				},
			}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				AdditionalComponents: map[string]v1beta1.DiffusionComponentSpec{
					"image_encoder": {Library: "transformers"},
				},
			}),
			wantCompatible: false,
			reasonContains: "diffusion component image_encoder missing in model",
		},
		{
			name: "additional component present but mismatched",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{
				AdditionalComponents: map[string]v1beta1.DiffusionComponentSpec{
					"image_encoder": {Library: "diffusers", Type: "CLIPVisionModel"},
				},
			}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				AdditionalComponents: map[string]v1beta1.DiffusionComponentSpec{
					"image_encoder": {Library: "transformers"},
				},
			}),
			wantCompatible: false,
			reasonContains: "image_encoder library mismatch",
		},
		{
			name: "full pipeline match across class, named components, and additional components",
			model: diffusionModel(&v1beta1.DiffusionPipelineSpec{
				ClassName:   ptr("StableDiffusionXLPipeline"),
				Scheduler:   &v1beta1.DiffusionComponentSpec{Library: "diffusers", Type: "EulerDiscreteScheduler"},
				TextEncoder: &v1beta1.DiffusionComponentSpec{Library: "transformers", Type: "CLIPTextModel"},
				VAE:         &v1beta1.DiffusionComponentSpec{Library: "diffusers", Type: "AutoencoderKL"},
				AdditionalComponents: map[string]v1beta1.DiffusionComponentSpec{
					"image_encoder": {Library: "transformers", Type: "CLIPVisionModel"},
				},
			}),
			runtime: diffusionRuntime(&v1beta1.DiffusionPipelineSpec{
				ClassName:   ptr("StableDiffusionXLPipeline"),
				Scheduler:   &v1beta1.DiffusionComponentSpec{Library: "diffusers"},
				TextEncoder: &v1beta1.DiffusionComponentSpec{Type: "CLIPTextModel"},
				VAE:         &v1beta1.DiffusionComponentSpec{Library: "diffusers", Type: "AutoencoderKL"},
				AdditionalComponents: map[string]v1beta1.DiffusionComponentSpec{
					"image_encoder": {Library: "transformers"},
				},
			}),
			wantCompatible: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report, err := matcher.GetCompatibilityDetails(tt.runtime, tt.model, isvc, "diffusion-rt")
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCompatible, report.IsCompatible)

			if tt.wantCompatible {
				assert.Empty(t, report.IncompatibilityReasons)
				return
			}
			assert.NotEmpty(t, report.IncompatibilityReasons)
			if tt.reasonContains != "" {
				assert.Contains(t, report.IncompatibilityReasons[0], tt.reasonContains)
			}
		})
	}
}
