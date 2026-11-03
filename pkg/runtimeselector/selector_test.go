package runtimeselector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func ptr[T any](v T) *T {
	return &v
}

// quantPtr / opPtr / fmtPtr / fwPtr exist because v1beta1.ModelQuantization
// and v1beta1.RuntimeSelectorOperator are named string types that ptr[string]
// can't satisfy directly. ptr() handles every other case.
func quantPtr(s string) *v1beta1.ModelQuantization {
	q := v1beta1.ModelQuantization(s)
	return &q
}

func opPtr(s string) *v1beta1.RuntimeSelectorOperator {
	o := v1beta1.RuntimeSelectorOperator(s)
	return &o
}

// fmtVersion / fwVersion build a SupportedModelFormat / ModelFormat /
// ModelFrameworkSpec quickly for the version-comparison test tables.
func fmtVersion(version string, op *v1beta1.RuntimeSelectorOperator) *v1beta1.ModelFormat {
	return &v1beta1.ModelFormat{Name: "pytorch", Version: ptr(version), Operator: op}
}

func fwVersion(version string, op *v1beta1.RuntimeSelectorOperator) *v1beta1.ModelFrameworkSpec {
	return &v1beta1.ModelFrameworkSpec{Name: "transformers", Version: ptr(version), Operator: op}
}

func TestNewSelector(t *testing.T) {
	fakeClient := fakeclient.New(t)
	selector := New(fakeClient)

	assert.NotNil(t, selector)
	_, ok := selector.(*defaultSelector)
	assert.True(t, ok)
}

func TestValidateModel(t *testing.T) {
	fakeClient := fakeclient.New(t)
	selector := New(fakeClient).(*defaultSelector)

	tests := []struct {
		name    string
		model   *v1beta1.BaseModelSpec
		wantErr bool
	}{
		{
			name:    "nil model",
			model:   nil,
			wantErr: true,
		},
		{
			name: "empty model format name",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "",
				},
			},
			wantErr: true,
		},
		{
			name: "valid model",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "pytorch",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := selector.validateModel(tt.model)
			if tt.wantErr {
				assert.Error(t, err)
				assert.True(t, IsModelValidationError(err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseModelSize(t *testing.T) {
	tests := []struct {
		name     string
		sizeStr  string
		expected float64
	}{
		{
			name:     "terabytes",
			sizeStr:  "1T",
			expected: 1_000_000_000_000,
		},
		{
			name:     "billions",
			sizeStr:  "7B",
			expected: 7_000_000_000,
		},
		{
			name:     "millions",
			sizeStr:  "350M",
			expected: 350_000_000,
		},
		{
			name:     "decimal billions",
			sizeStr:  "1.5B",
			expected: 1_500_000_000,
		},
		{
			name:     "no suffix",
			sizeStr:  "1000",
			expected: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseModelSize(tt.sizeStr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetModelFormatLabel(t *testing.T) {
	version := "1.0.0"
	arch := "transformer"
	quant := v1beta1.ModelQuantization("fp16")
	frameworkVersion := "4.0.0"

	tests := []struct {
		name     string
		model    *v1beta1.BaseModelSpec
		expected string
	}{
		{
			name: "basic format",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "pytorch",
				},
			},
			expected: "mt:pytorch",
		},
		{
			name: "format with version",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name:    "pytorch",
					Version: &version,
				},
			},
			expected: "mt:pytorch:1.0.0",
		},
		{
			name: "full spec",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name:    "pytorch",
					Version: &version,
				},
				ModelArchitecture: &arch,
				Quantization:      &quant,
				ModelFramework: &v1beta1.ModelFrameworkSpec{
					Name:    "transformers",
					Version: &frameworkVersion,
				},
			},
			expected: "mt:pytorch:1.0.0:transformer:fp16:transformers:4.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getModelFormatLabel(tt.model)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestErrorTypes(t *testing.T) {
	err := &RuntimeCompatibilityError{
		RuntimeName: "test-runtime",
		ModelName:   "test-model",
		ModelFormat: "pytorch",
		Reason:      "version mismatch",
	}
	assert.Contains(t, err.Error(), "test-runtime")
	assert.Contains(t, err.Error(), "test-model")
	assert.True(t, IsRuntimeCompatibilityError(err))

	noRuntimeErr := &NoRuntimeFoundError{
		ModelName:          "test-model",
		ModelFormat:        "pytorch",
		Namespace:          "default",
		TotalRuntimes:      5,
		NamespacedRuntimes: 2,
		ClusterRuntimes:    3,
	}
	assert.Contains(t, noRuntimeErr.Error(), "no runtime found")
	assert.Contains(t, noRuntimeErr.Error(), "5 runtimes")
	assert.True(t, IsNoRuntimeFoundError(noRuntimeErr))
}

func TestGetSupportingRuntimes(t *testing.T) {
	fakeClient := fakeclient.New(t)

	baseModels := []struct {
		name  string
		model *v1beta1.BaseModelSpec
	}{
		{
			name: "small-pytorch-model",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "pytorch",
				},
				ModelParameterSize: ptr("7B"),
			},
		},
		{
			name: "medium-onnx-model",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "onnx",
				},
				ModelParameterSize: ptr("13B"),
			},
		},
		{
			name: "large-tensorflow-model",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "tensorflow",
				},
				ModelParameterSize: ptr("70B"),
			},
		},
	}

	runtimes := []*v1beta1.ServingRuntime{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pytorch-rt",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 10,
						},
						AutoSelect: ptr(true),
						Priority:   ptr(int32(2)),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("1B"),
					Max: ptr("10B"),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "onnx-rt",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "onnx",
							Weight: 8,
						},
						AutoSelect: ptr(true),
						Priority:   ptr(int32(1)),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("10B"),
					Max: ptr("20B"),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tensorflow-rt",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "tensorflow",
							Weight: 12,
						},
						AutoSelect: ptr(true),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("50B"),
					Max: ptr("100B"),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "multi-format-rt",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 5,
						},
						AutoSelect: ptr(true),
						Priority:   ptr(int32(1)),
					},
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "onnx",
							Weight: 5,
						},
						AutoSelect: ptr(true),
						Priority:   ptr(int32(1)),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("1B"),
					Max: ptr("20B"),
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "disabled-rt",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				Disabled: ptr(true),
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name: "pytorch",
						},
						AutoSelect: ptr(true),
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-autoselect-rt",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 20,
						},
						AutoSelect: ptr(false),
					},
				},
			},
		},
	}

	clusterRuntimes := []*v1beta1.ClusterServingRuntime{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cluster-pytorch-rt",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 7,
						},
						AutoSelect: ptr(true),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("5B"),
					Max: ptr("15B"),
				},
			},
		},
	}

	ctx := context.Background()
	for _, rt := range runtimes {
		assert.NoError(t, fakeClient.Create(ctx, rt))
	}
	for _, rt := range clusterRuntimes {
		assert.NoError(t, fakeClient.Create(ctx, rt))
	}

	selector := New(fakeClient)

	tests := []struct {
		name                 string
		model                *v1beta1.BaseModelSpec
		expectedRuntimeNames []string
		expectedScores       []int64
		expectError          bool
	}{
		{
			name:  "small pytorch model - multiple compatible runtimes",
			model: baseModels[0].model,
			expectedRuntimeNames: []string{
				"pytorch-rt",         // score: 10 * 2 = 20 (namespace-scoped, higher priority)
				"multi-format-rt",    // score: 5 * 1 = 5 (namespace-scoped)
				"cluster-pytorch-rt", // score: 7 * 1 = 7 (cluster-scoped)
			},
			expectedScores: []int64{20, 5, 7},
		},
		{
			name:                 "medium onnx model",
			model:                baseModels[1].model,
			expectedRuntimeNames: []string{"onnx-rt", "multi-format-rt"},
			expectedScores:       []int64{8, 5},
		},
		{
			name:                 "large tensorflow model",
			model:                baseModels[2].model,
			expectedRuntimeNames: []string{"tensorflow-rt"},
			expectedScores:       []int64{12},
		},
		{
			name: "model with no compatible runtime",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "unknown-format",
				},
			},
			expectedRuntimeNames: []string{},
			expectError:          true,
		},
		{
			name: "model too large for any runtime",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "pytorch",
				},
				ModelParameterSize: ptr("200B"),
			},
			expectedRuntimeNames: []string{},
			expectError:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}
			matches, err := selector.GetCompatibleRuntimes(ctx, tt.model, isvc, "default")

			if tt.expectError {
				assert.Empty(t, matches)
				_, err := selector.SelectRuntime(ctx, tt.model, isvc)
				assert.Error(t, err)
				assert.True(t, IsNoRuntimeFoundError(err))
			} else {
				assert.NoError(t, err)
				assert.Len(t, matches, len(tt.expectedRuntimeNames))

				for i, match := range matches {
					assert.Equal(t, tt.expectedRuntimeNames[i], match.Name)
					assert.Equal(t, tt.expectedScores[i], match.Score)
				}
			}
		})
	}
}

func TestSortSupportedRuntime(t *testing.T) {
	baseModel := &v1beta1.BaseModelSpec{
		ModelFormat:        v1beta1.ModelFormat{Name: "test-format"},
		ModelFramework:     &v1beta1.ModelFrameworkSpec{Name: "test-framework"},
		ModelParameterSize: ptr("7B"),
	}

	tests := []struct {
		name          string
		runtimes      []RuntimeMatch
		expectedOrder []string
	}{
		{
			name: "sort by score - higher score wins",
			runtimes: []RuntimeMatch{
				{
					RuntimeSelection: RuntimeSelection{
						Name: "low-score-runtime",
						Spec: &v1beta1.ServingRuntimeSpec{
							SupportedModelFormats: []v1beta1.SupportedModelFormat{
								{
									ModelFormat: &v1beta1.ModelFormat{
										Name:   "test-format",
										Weight: 5,
									},
									Priority: ptr(int32(1)),
								},
							},
						},
						Score: 5,
					},
				},
				{
					RuntimeSelection: RuntimeSelection{
						Name: "high-score-runtime",
						Spec: &v1beta1.ServingRuntimeSpec{
							SupportedModelFormats: []v1beta1.SupportedModelFormat{
								{
									ModelFormat: &v1beta1.ModelFormat{
										Name:   "test-format",
										Weight: 10,
									},
									ModelFramework: &v1beta1.ModelFrameworkSpec{
										Name:   "test-framework",
										Weight: 8,
									},
									Priority: ptr(int32(2)),
								},
							},
						},
						Score: 36, // (10 + 8) * 2
					},
				},
			},
			expectedOrder: []string{"high-score-runtime", "low-score-runtime"},
		},
		{
			name: "sort by model size range when scores are equal",
			runtimes: []RuntimeMatch{
				{
					RuntimeSelection: RuntimeSelection{
						Name: "far-size-runtime",
						Spec: &v1beta1.ServingRuntimeSpec{
							ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
								Min: ptr("20B"),
								Max: ptr("30B"),
							},
						},
						Score: 10,
					},
				},
				{
					RuntimeSelection: RuntimeSelection{
						Name: "close-size-runtime",
						Spec: &v1beta1.ServingRuntimeSpec{
							ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
								Min: ptr("5B"),
								Max: ptr("10B"),
							},
						},
						Score: 10,
					},
				},
			},
			expectedOrder: []string{"close-size-runtime", "far-size-runtime"},
		},
		{
			name: "prefer namespace-scoped over cluster-scoped when equal",
			runtimes: []RuntimeMatch{
				{
					RuntimeSelection: RuntimeSelection{
						Name:      "cluster-runtime",
						IsCluster: true,
						Score:     10,
					},
				},
				{
					RuntimeSelection: RuntimeSelection{
						Name:      "namespace-runtime",
						IsCluster: false,
						Score:     10,
					},
				},
			},
			expectedOrder: []string{"namespace-runtime", "cluster-runtime"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scorer := NewDefaultRuntimeScorer(NewConfig(nil))
			selector := &defaultSelector{
				scorer: scorer,
			}

			selector.sortMatches(tt.runtimes, baseModel)

			got := make([]string, len(tt.runtimes))
			for i, rt := range tt.runtimes {
				got[i] = rt.Name
			}
			assert.Equal(t, tt.expectedOrder, got)
		})
	}
}

func TestScore(t *testing.T) {
	tests := []struct {
		name          string
		runtime       *v1beta1.ServingRuntimeSpec
		baseModel     *v1beta1.BaseModelSpec
		expectedScore int64
	}{
		{
			name: "exact match with weights and priority",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:    "PyTorch",
							Version: ptr("2.0.0"),
							Weight:  10,
						},
						ModelFramework: &v1beta1.ModelFrameworkSpec{
							Name:    "transformers",
							Version: ptr("4.0.0"),
							Weight:  5,
						},
						Priority:   ptr(int32(3)),
						AutoSelect: ptr(true),
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name:    "PyTorch",
					Version: ptr("2.0.0"),
				},
				ModelFramework: &v1beta1.ModelFrameworkSpec{
					Name:    "transformers",
					Version: ptr("4.0.0"),
				},
			},
			expectedScore: 45, // (10 * 3) + (5 * 3) = 30 + 15 = 45
		},
		{
			name: "format match only",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "PyTorch",
							Weight: 8,
						},
						ModelFramework: &v1beta1.ModelFrameworkSpec{
							Name:   "different-framework",
							Weight: 4,
						},
						Priority:   ptr(int32(2)),
						AutoSelect: ptr(true),
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "PyTorch",
				},
				ModelFramework: &v1beta1.ModelFrameworkSpec{
					Name: "transformers",
				},
			},
			expectedScore: 0, // Framework doesn't match, so no score
		},
		{
			name: "no match",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "TensorFlow",
							Weight: 10,
						},
						Priority:   ptr(int32(5)),
						AutoSelect: ptr(true),
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "PyTorch",
				},
			},
			expectedScore: 0, // No match
		},
		{
			name: "multiple formats, best match wins",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "PyTorch",
							Weight: 5,
						},
						ModelFramework: &v1beta1.ModelFrameworkSpec{
							Name:   "transformers",
							Weight: 8,
						},
						Priority:   ptr(int32(1)),
						AutoSelect: ptr(true),
					},
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "PyTorch",
							Weight: 10,
						},
						ModelFramework: &v1beta1.ModelFrameworkSpec{
							Name:   "transformers",
							Weight: 8,
						},
						Priority:   ptr(int32(2)),
						AutoSelect: ptr(true),
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "PyTorch",
				},
				ModelFramework: &v1beta1.ModelFrameworkSpec{
					Name: "transformers",
				},
			},
			expectedScore: 36, // Best match: (10 * 2) + (8 * 2) = 20 + 16 = 36
		},
		{
			name: "default priority when not specified",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "PyTorch",
							Weight: 12,
						},
						AutoSelect: ptr(true),
						// No priority specified, should default to 1
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "PyTorch",
				},
			},
			expectedScore: 12, // 12 * 1 (default priority) = 12
		},
		{
			name: "nil model framework in base model",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "PyTorch",
							Weight: 10,
						},
						ModelFramework: &v1beta1.ModelFrameworkSpec{
							Name:   "transformers",
							Weight: 5,
						},
						Priority:   ptr(int32(2)),
						AutoSelect: ptr(true),
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "PyTorch",
				},
				// ModelFramework is nil
			},
			expectedScore: 0, // model has no modelFramework, runtime has one, so they don't match
		},
		{
			name: "autoselect false - should get 0 score",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "PyTorch",
							Weight: 10,
						},
						Priority:   ptr(int32(2)),
						AutoSelect: ptr(false), // AutoSelect is false
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "PyTorch",
				},
			},
			expectedScore: 0, // AutoSelect is false, so score is 0
		},
		{
			// A format that leaves autoSelect unset is not offered for
			// automatic selection, so it adds nothing to the score.
			name: "format that leaves autoSelect unset does not count",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{ModelFormat: &v1beta1.ModelFormat{Name: "safetensors", Weight: 10}},
				},
			},
			baseModel:     &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}},
			expectedScore: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scorer := NewDefaultRuntimeScorer(NewConfig(nil))
			actualScore, err := scorer.CalculateScore(tt.runtime, tt.baseModel)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedScore, actualScore, "Score should match expected value")
		})
	}
}

func TestGetCompatibleRuntimes_ExcludeNoAutoSelect(t *testing.T) {
	// Create client and selector
	fakeClient := fakeclient.New(t)
	selector := New(fakeClient)

	rtNoAuto := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-no-auto", Namespace: "default"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 5}, AutoSelect: ptr(false)},
			},
		},
	}
	rtAuto := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-auto", Namespace: "default"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 10}, AutoSelect: ptr(true)},
			},
		},
	}

	ctx := context.Background()
	assert.NoError(t, fakeClient.Create(ctx, rtNoAuto))
	assert.NoError(t, fakeClient.Create(ctx, rtAuto))

	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}

	matches, err := selector.GetCompatibleRuntimes(ctx, model, isvc, "default")
	assert.NoError(t, err)
	assert.Len(t, matches, 1)
	assert.Equal(t, "rt-auto", matches[0].Name)
}

func TestSelectRuntime_NoRuntimeFoundError_Details(t *testing.T) {
	fakeClient := fakeclient.New(t)
	selector := New(fakeClient)

	rtWrongFormat := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-wrong-format", Namespace: "default"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{ModelFormat: &v1beta1.ModelFormat{Name: "onnx", Weight: 5}, AutoSelect: ptr(true)},
			},
		},
	}
	rtSizeTooSmall := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-size-too-small", Namespace: "default"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 5}, AutoSelect: ptr(true)},
			},
			ModelSizeRange: &v1beta1.ModelSizeRangeSpec{Min: ptr("1B"), Max: ptr("10B")},
		},
	}

	ctx := context.Background()
	assert.NoError(t, fakeClient.Create(ctx, rtWrongFormat))
	assert.NoError(t, fakeClient.Create(ctx, rtSizeTooSmall))

	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}, ModelParameterSize: ptr("70B")}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}

	matches, err := selector.GetCompatibleRuntimes(ctx, model, isvc, "default")
	assert.NoError(t, err)
	assert.Empty(t, matches)

	_, selErr := selector.SelectRuntime(ctx, model, isvc)
	assert.Error(t, selErr)
	assert.True(t, IsNoRuntimeFoundError(selErr))
	noRt := selErr.(*NoRuntimeFoundError)
	assert.Contains(t, noRt.ExcludedRuntimes, "rt-wrong-format")
	assert.Contains(t, noRt.ExcludedRuntimes, "rt-size-too-small")
}

func TestValidateRuntime_DisabledAndNoAutoSelect(t *testing.T) {
	fakeClient := fakeclient.New(t)
	selector := New(fakeClient)

	disabled := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-disabled", Namespace: "default"},
		Spec:       v1beta1.ServingRuntimeSpec{Disabled: ptr(true)},
	}
	// AutoSelect=false is OK for an explicit (non-auto) runtime pick.
	noAuto := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-no-auto", Namespace: "default"},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{ModelFormat: &v1beta1.ModelFormat{Name: "pytorch"}, AutoSelect: ptr(false)},
			},
		},
	}

	ctx := context.Background()
	assert.NoError(t, fakeClient.Create(ctx, disabled))
	assert.NoError(t, fakeClient.Create(ctx, noAuto))

	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}

	// Disabled should return RuntimeDisabledError
	err := selector.ValidateRuntime(ctx, "rt-disabled", model, isvc)
	assert.Error(t, err)
	assert.True(t, IsRuntimeDisabledError(err))

	// NoAutoSelect should return nil (compatible), even though auto-select is false
	assert.NoError(t, selector.ValidateRuntime(ctx, "rt-no-auto", model, isvc))
}

func TestGetSupportedModelFormat_CustomRuntime(t *testing.T) {
	fakeClient := fakeclient.New(t)
	selector := New(fakeClient).(*defaultSelector)
	ctx := context.Background()

	tests := []struct {
		name                 string
		runtime              *v1beta1.ServingRuntimeSpec
		model                *v1beta1.BaseModelSpec
		userSpecifiedRuntime bool
		expectFormat         bool
		expectedName         string
	}{
		{
			name: "auto-selection mode skips non-autoselect formats",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 10},
						AutoSelect:  ptr(false),
					},
				},
			},
			model:                &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}},
			userSpecifiedRuntime: false,
			expectFormat:         false,
		},
		{
			name: "auto-selection mode includes autoselect formats",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 10},
						AutoSelect:  ptr(true),
					},
				},
			},
			model:                &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}},
			userSpecifiedRuntime: false,
			expectFormat:         true,
			expectedName:         "pytorch",
		},
		{
			name: "user-selected runtime considers non-autoselect formats",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 10},
						AutoSelect:  ptr(false),
					},
				},
			},
			model:                &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}},
			userSpecifiedRuntime: true,
			expectFormat:         true,
			expectedName:         "pytorch",
		},
		{
			name: "user-selected runtime considers nil autoselect formats",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{Name: "onnx", Weight: 8},
						// AutoSelect is nil
					},
				},
			},
			model:                &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "onnx"}},
			userSpecifiedRuntime: true,
			expectFormat:         true,
			expectedName:         "onnx",
		},
		{
			name: "auto-selection mode skips nil autoselect formats",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{Name: "onnx", Weight: 8},
						// AutoSelect is nil
					},
				},
			},
			model:                &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "onnx"}},
			userSpecifiedRuntime: false,
			expectFormat:         false,
		},
		{
			name: "user-selected runtime picks best matching format among multiple",
			runtime: &v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 5},
						AutoSelect:  ptr(false),
					},
					{
						ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 15},
						AutoSelect:  ptr(false),
					},
				},
			},
			model:                &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}},
			userSpecifiedRuntime: true,
			expectFormat:         true,
			expectedName:         "pytorch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.GetSupportedModelFormat(ctx, tt.runtime, tt.model, tt.userSpecifiedRuntime)
			if tt.expectFormat {
				assert.NotNil(t, result, "expected a supported model format to be returned")
				assert.Equal(t, tt.expectedName, result.ModelFormat.Name)
			} else {
				// When no format matches, result should have zero score (empty format)
				if result != nil && result.ModelFormat != nil {
					assert.Fail(t, "expected no matching format, but got one", "format: %v", result.ModelFormat.Name)
				}
			}
		})
	}
}

func TestSupportedRuntimeWithAC(t *testing.T) {
	// Create a fake fakeClient with our custom types registered
	fakeClient := fakeclient.New(t)
	ac1 := "nvidia-tesla-t4"
	ac2 := "H100"
	// Create test base models with different formats and sizes
	baseModels := []struct {
		name  string
		model *v1beta1.BaseModelSpec
	}{
		{
			name: "small-pytorch-model",
			model: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "pytorch",
				},
				ModelParameterSize: ptr("7B"),
			},
		},
	}
	// Create test serving runtimes with different capabilities
	runtimes := []*v1beta1.ServingRuntime{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pytorch-rt",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 10,
						},
						AutoSelect: ptr(true),
						Priority:   ptr(int32(2)),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("1B"),
					Max: ptr("10B"),
				},
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-tesla-t4", "nvidia-tesla-v100", "nvidia-a100"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pytorch-rt-1",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 8,
						},
						AutoSelect: ptr(true),
						Priority:   ptr(int32(1)),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("1B"),
					Max: ptr("8B"),
				},
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-tesla-t4", "nvidia-tesla-v100", "nvidia-a100"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pytorch-rt-2",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 12,
						},
						AutoSelect: ptr(true),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("50B"),
					Max: ptr("100B"),
				},
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-tesla-t4", "nvidia-tesla-v100", "nvidia-a100"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pytorch-rt-3",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 12,
						},
						AutoSelect: ptr(true),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("1B"),
					Max: ptr("10B"),
				},
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"H100"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pytorch-rt-4",
				Namespace: "default",
			},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{
						ModelFormat: &v1beta1.ModelFormat{
							Name:   "pytorch",
							Weight: 12,
						},
						AutoSelect: ptr(true),
					},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
					Min: ptr("1B"),
					Max: ptr("10B"),
				},
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"H100", "nvidia-tesla-t4", "nvidia-tesla-v100", "nvidia-a100"},
				},
			},
		},
	}
	infereceServices := []*v1beta1.InferenceService{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-ac",
				Namespace: "default",
			},
			Spec: v1beta1.InferenceServiceSpec{
				AcceleratorSelector: &v1beta1.AcceleratorSelector{
					AcceleratorClass: &ac1,
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-without-ac",
				Namespace: "default",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-ac-h100",
				Namespace: "default",
			},
			Spec: v1beta1.InferenceServiceSpec{
				AcceleratorSelector: &v1beta1.AcceleratorSelector{
					AcceleratorClass: &ac2,
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-engine-ac",
				Namespace: "default",
			},
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					AcceleratorOverride: &v1beta1.AcceleratorSelector{
						AcceleratorClass: &ac1,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-engine-without-ac",
				Namespace: "default",
			},
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-decoder-ac",
				Namespace: "default",
			},
			Spec: v1beta1.InferenceServiceSpec{
				Decoder: &v1beta1.DecoderSpec{
					AcceleratorOverride: &v1beta1.AcceleratorSelector{
						AcceleratorClass: &ac1,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-decoder-without-ac",
				Namespace: "default",
			},
			Spec: v1beta1.InferenceServiceSpec{
				Decoder: &v1beta1.DecoderSpec{},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-decoder-engine-diff-ac",
				Namespace: "default",
			},
			Spec: v1beta1.InferenceServiceSpec{
				Decoder: &v1beta1.DecoderSpec{
					AcceleratorOverride: &v1beta1.AcceleratorSelector{
						AcceleratorClass: &ac1,
					},
				},
				Engine: &v1beta1.EngineSpec{
					AcceleratorOverride: &v1beta1.AcceleratorSelector{
						AcceleratorClass: &ac2,
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "isvc-with-annotation-ac",
				Namespace: "default",
				Annotations: map[string]string{
					"ome.io/accelerator-class": "nvidia-tesla-t4",
				},
			},
			Spec: v1beta1.InferenceServiceSpec{},
		},
	}
	// Add all runtimes to the fake client
	ctx := context.Background()
	for _, rt := range runtimes {
		assert.NoError(t, fakeClient.Create(ctx, rt))
	}
	for _, isvc := range infereceServices {
		assert.NoError(t, fakeClient.Create(ctx, isvc))
	}

	// Create selector
	selector := New(fakeClient)
	tests := []struct {
		name                 string
		model                *v1beta1.BaseModelSpec
		inferenceService     *v1beta1.InferenceService
		expectedRuntimeNames []string
		expectError          bool
	}{
		{
			name:             "select with inference service ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[0],
			expectedRuntimeNames: []string{
				"pytorch-rt",
				"pytorch-rt-4",
				"pytorch-rt-1",
			},
			expectError: false,
		},
		{

			name:             "select without inference service ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[1],
			expectedRuntimeNames: []string{
				"pytorch-rt",
				"pytorch-rt-3",
				"pytorch-rt-4",
				"pytorch-rt-1",
			},
			expectError: false,
		},
		{

			name:             "select with inference service ac-h100",
			model:            baseModels[0].model,
			inferenceService: infereceServices[2],
			expectedRuntimeNames: []string{
				"pytorch-rt-3",
				"pytorch-rt-4",
			},
			expectError: false,
		},
		{

			name:             "select with inference service engine-ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[3],
			expectedRuntimeNames: []string{
				"pytorch-rt",
				"pytorch-rt-4",
				"pytorch-rt-1",
			},
			expectError: false,
		},
		{

			name:             "select without inference service engine-ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[4],
			expectedRuntimeNames: []string{
				"pytorch-rt",
				"pytorch-rt-3",
				"pytorch-rt-4",
				"pytorch-rt-1",
			},
			expectError: false,
		},
		{

			name:             "select without inference service decoder-ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[5],
			expectedRuntimeNames: []string{
				"pytorch-rt",
				"pytorch-rt-4",
				"pytorch-rt-1",
			},
			expectError: false,
		},
		{
			name:             "select without inference service decoder-ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[6],
			expectedRuntimeNames: []string{
				"pytorch-rt",
				"pytorch-rt-3",
				"pytorch-rt-4",
				"pytorch-rt-1",
			},
			expectError: false,
		},
		{

			name:             "select without inference service decoder-engine-diff-ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[7],
			expectedRuntimeNames: []string{
				"pytorch-rt-4",
			},
			expectError: false,
		},
		{

			name:             "select with inference service annotation ac",
			model:            baseModels[0].model,
			inferenceService: infereceServices[8],
			expectedRuntimeNames: []string{
				"pytorch-rt",
				"pytorch-rt-4",
				"pytorch-rt-1",
			},
			expectError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := tt.inferenceService
			// Ensure fields used by matcher are non-nil
			matches, err := selector.GetCompatibleRuntimes(ctx, tt.model, isvc, "default")
			if tt.expectError {
				assert.Empty(t, matches)
				// Try SelectRuntime to get the error
				_, err := selector.SelectRuntime(ctx, tt.model, isvc)
				assert.Error(t, err)
				assert.True(t, IsNoRuntimeFoundError(err))
			} else {
				assert.NoError(t, err)
				assert.Len(t, matches, len(tt.expectedRuntimeNames))

				for i, match := range matches {
					assert.Equal(t, tt.expectedRuntimeNames[i], match.Name)
				}
			}
		})
	}
}

// A lean InferenceService carries no model at all; both selector entry
// points must reject a nil model with a typed error instead of
// dereferencing it.
func TestSelectRuntime_NilModelReturnsError(t *testing.T) {
	selector := New(fakeclient.New(t))
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "lean", Namespace: "default"}}

	_, err := selector.SelectRuntime(context.Background(), nil, isvc)
	assert.Error(t, err)
	assert.IsType(t, &ModelValidationError{}, err)
}

func TestValidateRuntime_NilModelReturnsError(t *testing.T) {
	selector := New(fakeclient.New(t))
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "lean", Namespace: "default"}}

	err := selector.ValidateRuntime(context.Background(), "some-runtime", nil, isvc)
	assert.Error(t, err)
	assert.IsType(t, &ModelValidationError{}, err)
}

// A runtime offers a format for automatic selection only by marking it
// autoSelect: true. The ranking and the rendered format read that mark the
// same way: a runtime whose only fitting format leaves it unset is not
// chosen, so a runtime that opted in wins, and alone it leaves the service
// without a runtime rather than with one that renders no format.
func TestSelectRuntime_OnlyFittingFormatLeavesAutoSelectUnset_OptedInRuntimeWins(t *testing.T) {
	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"}}
	// Opted in for onnx only; the safetensors entry leaves autoSelect unset.
	notOptedIn := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "a-runtime", Namespace: "default"},
		Spec: v1beta1.ServingRuntimeSpec{SupportedModelFormats: []v1beta1.SupportedModelFormat{
			{ModelFormat: &v1beta1.ModelFormat{Name: "onnx", Weight: 10}, AutoSelect: ptr(true)},
			{ModelFormat: &v1beta1.ModelFormat{Name: "safetensors", Weight: 10}},
		}},
	}
	optedIn := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "b-runtime", Namespace: "default"},
		Spec: v1beta1.ServingRuntimeSpec{SupportedModelFormats: []v1beta1.SupportedModelFormat{
			{ModelFormat: &v1beta1.ModelFormat{Name: "safetensors", Weight: 10}, AutoSelect: ptr(true)},
		}},
	}
	ctx := context.Background()

	t.Run("the runtime that opted in wins", func(t *testing.T) {
		fakeClient := fakeclient.New(t)
		assert.NoError(t, fakeClient.Create(ctx, notOptedIn.DeepCopy()))
		assert.NoError(t, fakeClient.Create(ctx, optedIn.DeepCopy()))
		selector := New(fakeClient)

		selection, err := selector.SelectRuntime(ctx, model, isvc)
		if assert.NoError(t, err) {
			assert.Equal(t, "b-runtime", selection.Name)
			rendered := selector.GetSupportedModelFormat(ctx, selection.Spec, model, false)
			if assert.NotNil(t, rendered, "the chosen runtime renders the format it was chosen by") {
				assert.Equal(t, "safetensors", rendered.ModelFormat.Name)
			}
		}
	})

	t.Run("alone it is not chosen", func(t *testing.T) {
		fakeClient := fakeclient.New(t)
		assert.NoError(t, fakeClient.Create(ctx, notOptedIn.DeepCopy()))
		selector := New(fakeClient)

		selection, err := selector.SelectRuntime(ctx, model, isvc)
		if !assert.IsType(t, &NoRuntimeFoundError{}, err) && selection != nil {
			t.Logf("chosen %s, rendered format %v", selection.Name, selector.GetSupportedModelFormat(ctx, selection.Spec, model, false))
		}
	})
}

// An auto-selected runtime renders the format that gave it its score. The
// ranking scores each format at its own priority, so the rendered format is
// read the same way; a named runtime overrides the priorities and keeps the
// default for every format.
func TestGetSupportedModelFormat_AutoSelectedRuntime_RendersTheScoringFormat(t *testing.T) {
	tensorParallel := func(size int64) map[string]*v1beta1.AcceleratorModelConfig {
		return map[string]*v1beta1.AcceleratorModelConfig{
			"class-a": {TensorParallelismOverride: &v1beta1.TensorParallelismConfig{TensorParallelSize: ptr(size)}},
		}
	}
	renderedTensorParallel := func(format *v1beta1.SupportedModelFormat) int64 {
		return *format.AcceleratorConfig["class-a"].TensorParallelismOverride.TensorParallelSize
	}
	runtime := &v1beta1.ServingRuntimeSpec{SupportedModelFormats: []v1beta1.SupportedModelFormat{
		{
			ModelFormat:       &v1beta1.ModelFormat{Name: "safetensors", Weight: 10},
			AutoSelect:        ptr(true),
			Priority:          ptr(int32(3)),
			AcceleratorConfig: tensorParallel(8),
		},
		{
			ModelFormat:       &v1beta1.ModelFormat{Name: "safetensors", Weight: 20},
			AutoSelect:        ptr(true),
			Priority:          ptr(int32(1)),
			AcceleratorConfig: tensorParallel(4),
		},
	}}
	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}}
	selector := New(fakeclient.New(t)).(*defaultSelector)
	ctx := context.Background()

	score, err := selector.scorer.CalculateScore(runtime, model)
	assert.NoError(t, err)
	assert.Equal(t, int64(30), score, "the ranking scores the priority 3 format: 10 x 3")

	t.Run("auto-selected runtime renders the priority 3 format", func(t *testing.T) {
		rendered := selector.GetSupportedModelFormat(ctx, runtime, model, false)
		if assert.NotNil(t, rendered) {
			assert.Equal(t, int64(8), renderedTensorParallel(rendered))
		}
	})

	t.Run("named runtime keeps the default priority", func(t *testing.T) {
		rendered := selector.GetSupportedModelFormat(ctx, runtime, model, true)
		if assert.NotNil(t, rendered) {
			assert.Equal(t, int64(4), renderedTensorParallel(rendered))
		}
	})
}
