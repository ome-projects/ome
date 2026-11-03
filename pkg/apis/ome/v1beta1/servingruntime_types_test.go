package v1beta1

import (
	"testing"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func TestSupportedModelFormat_GetAcceleratorConfig(t *testing.T) {
	testCases := []struct {
		name             string
		format           *SupportedModelFormat
		acceleratorClass string
		expected         *AcceleratorModelConfig
	}{
		{
			name: "returns config when accelerator class exists",
			format: &SupportedModelFormat{
				Name: "test-format",
				AcceleratorConfig: map[string]*AcceleratorModelConfig{
					"nvidia-a100": {
						MinMemoryPerBillionParams: proto.Int64(1000),
						RuntimeArgsOverride:       []string{"--arg1", "--arg2"},
						EnvironmentOverride: map[string]string{
							"ENV_VAR": "value",
						},
					},
					"nvidia-h100": {
						MinMemoryPerBillionParams: proto.Int64(800),
					},
				},
			},
			acceleratorClass: "nvidia-a100",
			expected: &AcceleratorModelConfig{
				MinMemoryPerBillionParams: proto.Int64(1000),
				RuntimeArgsOverride:       []string{"--arg1", "--arg2"},
				EnvironmentOverride: map[string]string{
					"ENV_VAR": "value",
				},
			},
		},
		{
			name: "returns nil when accelerator class does not exist",
			format: &SupportedModelFormat{
				Name: "test-format",
				AcceleratorConfig: map[string]*AcceleratorModelConfig{
					"nvidia-a100": {
						MinMemoryPerBillionParams: proto.Int64(1000),
					},
				},
			},
			acceleratorClass: "nvidia-v100",
			expected:         nil,
		},
		{
			name: "returns nil when AcceleratorConfig is nil",
			format: &SupportedModelFormat{
				Name:              "test-format",
				AcceleratorConfig: nil,
			},
			acceleratorClass: "nvidia-a100",
			expected:         nil,
		},
		{
			name: "returns nil when AcceleratorConfig is empty",
			format: &SupportedModelFormat{
				Name:              "test-format",
				AcceleratorConfig: map[string]*AcceleratorModelConfig{},
			},
			acceleratorClass: "nvidia-a100",
			expected:         nil,
		},
		{
			name: "returns config with complex tensor parallelism settings",
			format: &SupportedModelFormat{
				Name: "test-format",
				AcceleratorConfig: map[string]*AcceleratorModelConfig{
					"nvidia-h100": {
						MinMemoryPerBillionParams: proto.Int64(500),
						TensorParallelismOverride: &TensorParallelismConfig{
							TensorParallelSize:   proto.Int64(4),
							PipelineParallelSize: proto.Int64(2),
							DataParallelSize:     proto.Int64(1),
						},
						RuntimeArgsOverride: []string{"--tensor-parallel-size=4"},
						EnvironmentOverride: map[string]string{
							"TP_SIZE": "4",
							"PP_SIZE": "2",
						},
					},
				},
			},
			acceleratorClass: "nvidia-h100",
			expected: &AcceleratorModelConfig{
				MinMemoryPerBillionParams: proto.Int64(500),
				TensorParallelismOverride: &TensorParallelismConfig{
					TensorParallelSize:   proto.Int64(4),
					PipelineParallelSize: proto.Int64(2),
					DataParallelSize:     proto.Int64(1),
				},
				RuntimeArgsOverride: []string{"--tensor-parallel-size=4"},
				EnvironmentOverride: map[string]string{
					"TP_SIZE": "4",
					"PP_SIZE": "2",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := tc.format.GetAcceleratorConfig(tc.acceleratorClass)
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}

func TestServingRuntimeSpec_IsDisabled(t *testing.T) {
	testCases := []struct {
		name     string
		spec     *ServingRuntimeSpec
		expected bool
	}{
		{
			name: "returns true when disabled is explicitly true",
			spec: &ServingRuntimeSpec{
				Disabled: proto.Bool(true),
			},
			expected: true,
		},
		{
			name: "returns false when disabled is explicitly false",
			spec: &ServingRuntimeSpec{
				Disabled: proto.Bool(false),
			},
			expected: false,
		},
		{
			name: "returns false when disabled is nil",
			spec: &ServingRuntimeSpec{
				Disabled: nil,
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := tc.spec.IsDisabled()
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}

func TestSupportedModelFormat_IsAutoSelectEnabled(t *testing.T) {
	testCases := []struct {
		name     string
		format   *SupportedModelFormat
		expected bool
	}{
		{
			name: "returns true when auto select is explicitly true",
			format: &SupportedModelFormat{
				Name:       "pytorch",
				AutoSelect: proto.Bool(true),
			},
			expected: true,
		},
		{
			name: "returns false when auto select is explicitly false",
			format: &SupportedModelFormat{
				Name:       "pytorch",
				AutoSelect: proto.Bool(false),
			},
			expected: false,
		},
		{
			name: "returns false when auto select is nil",
			format: &SupportedModelFormat{
				Name:       "pytorch",
				AutoSelect: nil,
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := tc.format.IsAutoSelectEnabled()
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}
