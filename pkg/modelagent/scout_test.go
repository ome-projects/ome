package modelagent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
)

// Test the shouldDownloadModel function
func TestShouldDownloadModel(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Set up test node
	testNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				constants.NodeInstanceShapeLabel:           "GPU.A10.2",
				constants.DeprecatedNodeInstanceShapeLabel: "GPU.A10.2",
				"accelerator": "nvidia",
				"gpu-model":   "a10",
				"region":      "us-west-2",
			},
			Annotations: map[string]string{
				constants.TargetInstanceShapes: "GPU.A10.2,GPU.A100.8",
			},
		},
	}

	// Create a test scount with minimal fields needed for the test
	scount := &Scout{
		nodeName:       "test-node",
		nodeInfo:       testNode,
		nodeShapeAlias: "a10",
		logger:         sugaredLogger,
	}

	// Define test cases
	testCases := []struct {
		name           string
		storageSpec    *v1beta1.StorageSpec
		expectedResult bool
		description    string
	}{
		{
			name:           "nil storage spec",
			storageSpec:    nil,
			expectedResult: true,
			description:    "Nil StorageSpec should default to true",
		},
		{
			name:           "empty storage spec",
			storageSpec:    &v1beta1.StorageSpec{},
			expectedResult: true,
			description:    "Empty StorageSpec should default to true",
		},
		{
			name: "matching node selector",
			storageSpec: &v1beta1.StorageSpec{
				NodeSelector: map[string]string{
					"gpu-model": "a10",
				},
			},
			expectedResult: true,
			description:    "NodeSelector matches node labels",
		},
		{
			name: "non-matching node selector",
			storageSpec: &v1beta1.StorageSpec{
				NodeSelector: map[string]string{
					"gpu-model": "a100",
				},
			},
			expectedResult: false,
			description:    "NodeSelector doesn't match node labels",
		},
		{
			name: "matching required node affinity - In operator",
			storageSpec: &v1beta1.StorageSpec{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "gpu-model",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"a10", "a100"},
									},
								},
							},
						},
					},
				},
			},
			expectedResult: true,
			description:    "NodeAffinity with In operator matching node labels",
		},
		{
			name: "non-matching required node affinity - In operator",
			storageSpec: &v1beta1.StorageSpec{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "gpu-model",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"a100", "h100"},
									},
								},
							},
						},
					},
				},
			},
			expectedResult: false,
			description:    "NodeAffinity with In operator not matching node labels",
		},
		{
			name: "matching required node affinity - Exists operator",
			storageSpec: &v1beta1.StorageSpec{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "accelerator",
										Operator: corev1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			expectedResult: true,
			description:    "NodeAffinity with Exists operator matching node labels",
		},
		{
			name: "non-matching required node affinity - Exists operator",
			storageSpec: &v1beta1.StorageSpec{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "non-existent-label",
										Operator: corev1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			expectedResult: false,
			description:    "NodeAffinity with Exists operator not matching node labels",
		},
		{
			name: "matching multiple node selector terms - OR relationship",
			storageSpec: &v1beta1.StorageSpec{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "non-existent-label",
										Operator: corev1.NodeSelectorOpExists,
									},
								},
							},
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "gpu-model",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"a10"},
									},
								},
							},
						},
					},
				},
			},
			expectedResult: true,
			description:    "One of multiple NodeSelectorTerms matches (OR relationship)",
		},
		{
			name: "matching multiple match expressions - AND relationship",
			storageSpec: &v1beta1.StorageSpec{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "gpu-model",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"a10"},
									},
									{
										Key:      "accelerator",
										Operator: corev1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			expectedResult: true,
			description:    "Multiple MatchExpressions in one term all match (AND relationship)",
		},
		{
			name: "non-matching multiple match expressions - AND relationship",
			storageSpec: &v1beta1.StorageSpec{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "gpu-model",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"a10"},
									},
									{
										Key:      "non-existent-label",
										Operator: corev1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			expectedResult: false,
			description:    "One of multiple MatchExpressions doesn't match (AND relationship)",
		},
		{
			name: "node selector + node affinity - both match",
			storageSpec: &v1beta1.StorageSpec{
				NodeSelector: map[string]string{
					"gpu-model": "a10",
				},
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "accelerator",
										Operator: corev1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			expectedResult: true,
			description:    "Both NodeSelector and NodeAffinity match",
		},
		{
			name: "node selector + node affinity - one doesn't match",
			storageSpec: &v1beta1.StorageSpec{
				NodeSelector: map[string]string{
					"gpu-model": "a100", // This doesn't match
				},
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "accelerator",
										Operator: corev1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			expectedResult: false,
			description:    "NodeSelector doesn't match but NodeAffinity does",
		},
		{
			name: "fallback to annotation - matching",
			storageSpec: &v1beta1.StorageSpec{
				StorageUri: stringPtr("oci://foo/bar"),
			},
			expectedResult: true,
			description:    "Empty node selectors should fallback to annotation check (which matches)",
		},
	}

	// Run tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := scount.shouldDownloadModel(tc.storageSpec)
			if result != tc.expectedResult {
				t.Errorf("%s: Expected %v but got %v. %s", tc.name, tc.expectedResult, result, tc.description)
			}
		})
	}
}

// Helper function to return a string pointer
func stringPtr(s string) *string {
	return &s
}

// Test the nodeMatchesExpression function
func TestNodeMatchesExpression(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Set up test node
	testNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				"gpu-model": "a10",
				"region":    "us-west-2",
				"memory":    "32Gi",
				"cpu":       "8",
			},
		},
	}

	// Create a test scount with minimal fields needed
	scount := &Scout{
		nodeName: "test-node",
		nodeInfo: testNode,
		logger:   sugaredLogger,
	}

	// Define test cases
	testCases := []struct {
		name           string
		expr           corev1.NodeSelectorRequirement
		expectedResult bool
		description    string
	}{
		{
			name: "In operator - matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "gpu-model",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"a10", "a100"},
			},
			expectedResult: true,
			description:    "In operator should match when label value is in the values list",
		},
		{
			name: "In operator - non-matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "gpu-model",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"a100", "h100"},
			},
			expectedResult: false,
			description:    "In operator should not match when label value is not in the values list",
		},
		{
			name: "NotIn operator - matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "gpu-model",
				Operator: corev1.NodeSelectorOpNotIn,
				Values:   []string{"a100", "h100"},
			},
			expectedResult: true,
			description:    "NotIn operator should match when label value is not in the values list",
		},
		{
			name: "NotIn operator - non-matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "gpu-model",
				Operator: corev1.NodeSelectorOpNotIn,
				Values:   []string{"a10", "h100"},
			},
			expectedResult: false,
			description:    "NotIn operator should not match when label value is in the values list",
		},
		{
			name: "Exists operator - matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "gpu-model",
				Operator: corev1.NodeSelectorOpExists,
			},
			expectedResult: true,
			description:    "Exists operator should match when label exists",
		},
		{
			name: "Exists operator - non-matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "non-existent-label",
				Operator: corev1.NodeSelectorOpExists,
			},
			expectedResult: false,
			description:    "Exists operator should not match when label doesn't exist",
		},
		{
			name: "DoesNotExist operator - matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "non-existent-label",
				Operator: corev1.NodeSelectorOpDoesNotExist,
			},
			expectedResult: true,
			description:    "DoesNotExist operator should match when label doesn't exist",
		},
		{
			name: "DoesNotExist operator - non-matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "gpu-model",
				Operator: corev1.NodeSelectorOpDoesNotExist,
			},
			expectedResult: false,
			description:    "DoesNotExist operator should not match when label exists",
		},
		{
			name: "Gt operator - matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "cpu",
				Operator: corev1.NodeSelectorOpGt,
				Values:   []string{"4"},
			},
			expectedResult: true,
			description:    "Gt operator should match when label value is greater than the specified value",
		},
		{
			name: "Gt operator - non-matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "cpu",
				Operator: corev1.NodeSelectorOpGt,
				Values:   []string{"16"},
			},
			expectedResult: false,
			description:    "Gt operator should not match when label value is not greater than the specified value",
		},
		{
			name: "Lt operator - matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "cpu",
				Operator: corev1.NodeSelectorOpLt,
				Values:   []string{"16"},
			},
			expectedResult: true,
			description:    "Lt operator should match when label value is less than the specified value",
		},
		{
			name: "Lt operator - non-matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "cpu",
				Operator: corev1.NodeSelectorOpLt,
				Values:   []string{"4"},
			},
			expectedResult: false,
			description:    "Lt operator should not match when label value is not less than the specified value",
		},
		{
			name: "Gt operator - empty label value",
			expr: corev1.NodeSelectorRequirement{
				Key:      "non-existent-label",
				Operator: corev1.NodeSelectorOpGt,
				Values:   []string{"4"},
			},
			expectedResult: false,
			description:    "Gt operator should not match when label doesn't exist",
		},
		{
			name: "Lt operator - empty label value",
			expr: corev1.NodeSelectorRequirement{
				Key:      "non-existent-label",
				Operator: corev1.NodeSelectorOpLt,
				Values:   []string{"4"},
			},
			expectedResult: false,
			description:    "Lt operator should not match when label doesn't exist",
		},
		{
			name: "Gt operator - empty values",
			expr: corev1.NodeSelectorRequirement{
				Key:      "cpu",
				Operator: corev1.NodeSelectorOpGt,
				Values:   []string{},
			},
			expectedResult: false,
			description:    "Gt operator should not match when values are empty",
		},
		{
			name: "Lt operator - empty values",
			expr: corev1.NodeSelectorRequirement{
				Key:      "cpu",
				Operator: corev1.NodeSelectorOpLt,
				Values:   []string{},
			},
			expectedResult: false,
			description:    "Lt operator should not match when values are empty",
		},
		{
			name: "Unknown operator",
			expr: corev1.NodeSelectorRequirement{
				Key:      "cpu",
				Operator: "UnknownOperator",
				Values:   []string{"8"},
			},
			expectedResult: false,
			description:    "Unknown operator should default to false",
		},
	}

	// Run tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := scount.nodeMatchesExpression(tc.expr)
			if result != tc.expectedResult {
				t.Errorf("%s: Expected %v but got %v. %s", tc.name, tc.expectedResult, result, tc.description)
			}
		})
	}
}

// Test the nodeMatchesSelectorTerm function
func TestNodeMatchesSelectorTerm(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Set up test node
	testNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				"gpu-model":   "a10",
				"region":      "us-west-2",
				"accelerator": "nvidia",
			},
		},
	}

	// Create a test scount with minimal fields needed
	scount := &Scout{
		nodeName: "test-node",
		nodeInfo: testNode,
		logger:   sugaredLogger,
	}

	// Define test cases
	testCases := []struct {
		name           string
		term           corev1.NodeSelectorTerm
		expectedResult bool
		description    string
	}{
		{
			name: "single match expression - matching",
			term: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "gpu-model",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"a10"},
					},
				},
			},
			expectedResult: true,
			description:    "Should match when single match expression matches",
		},
		{
			name: "single match expression - non-matching",
			term: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "gpu-model",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"a100"},
					},
				},
			},
			expectedResult: false,
			description:    "Should not match when single match expression doesn't match",
		},
		{
			name: "multiple match expressions - all matching (AND)",
			term: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "gpu-model",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"a10"},
					},
					{
						Key:      "accelerator",
						Operator: corev1.NodeSelectorOpExists,
					},
				},
			},
			expectedResult: true,
			description:    "Should match when all match expressions match (AND relationship)",
		},
		{
			name: "multiple match expressions - one non-matching (AND)",
			term: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "gpu-model",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"a10"},
					},
					{
						Key:      "non-existent-label",
						Operator: corev1.NodeSelectorOpExists,
					},
				},
			},
			expectedResult: false,
			description:    "Should not match when any match expression doesn't match (AND relationship)",
		},
	}

	// Run tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := scount.nodeMatchesSelectorTerm(tc.term)
			if result != tc.expectedResult {
				t.Errorf("%s: Expected %v but got %v. %s", tc.name, tc.expectedResult, result, tc.description)
			}
		})
	}
}

// Test the NodeMatchesSelectorTerm function with MatchFields
func TestNodeMatchesSelectorTermWithMatchFields(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Set up test node
	testNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				"gpu-model":   "a10",
				"accelerator": "nvidia",
			},
		},
	}

	// Create a test scount with minimal fields needed
	scount := &Scout{
		nodeName: "test-node",
		nodeInfo: testNode,
		logger:   sugaredLogger,
	}

	// Define test cases
	testCases := []struct {
		name           string
		term           corev1.NodeSelectorTerm
		expectedResult bool
		description    string
	}{
		{
			name: "match field for metadata.name - matching",
			term: corev1.NodeSelectorTerm{
				MatchFields: []corev1.NodeSelectorRequirement{
					{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"test-node"},
					},
				},
			},
			expectedResult: true,
			description:    "Should match when field metadata.name matches",
		},
		{
			name: "match field for metadata.name - non-matching",
			term: corev1.NodeSelectorTerm{
				MatchFields: []corev1.NodeSelectorRequirement{
					{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"other-node"},
					},
				},
			},
			expectedResult: false,
			description:    "Should not match when field metadata.name doesn't match",
		},
		{
			name: "match expressions and match fields - both match",
			term: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "gpu-model",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"a10"},
					},
				},
				MatchFields: []corev1.NodeSelectorRequirement{
					{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"test-node"},
					},
				},
			},
			expectedResult: true,
			description:    "Should match when both match expressions and match fields match",
		},
		{
			name: "match expressions and match fields - match expressions don't match",
			term: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "gpu-model",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"a100"},
					},
				},
				MatchFields: []corev1.NodeSelectorRequirement{
					{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"test-node"},
					},
				},
			},
			expectedResult: false,
			description:    "Should not match when match expressions don't match even if match fields match",
		},
		{
			name: "match expressions and match fields - match fields don't match",
			term: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "gpu-model",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"a10"},
					},
				},
				MatchFields: []corev1.NodeSelectorRequirement{
					{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"other-node"},
					},
				},
			},
			expectedResult: false,
			description:    "Should not match when match fields don't match even if match expressions match",
		},
		{
			name: "match field with NotIn operator - matching",
			term: corev1.NodeSelectorTerm{
				MatchFields: []corev1.NodeSelectorRequirement{
					{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpNotIn,
						Values:   []string{"other-node"},
					},
				},
			},
			expectedResult: true,
			description:    "Should match when field metadata.name is not in the values",
		},
	}

	// Run tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := scount.nodeMatchesSelectorTerm(tc.term)
			if result != tc.expectedResult {
				t.Errorf("%s: Expected %v but got %v. %s", tc.name, tc.expectedResult, result, tc.description)
			}
		})
	}
}

// Test complex fallback scenarios for the shouldDownloadModel function
func TestShouldDownloadModelFallback(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Test cases for different annotation scenarios
	testCases := []struct {
		name            string
		nodeAnnotations map[string]string
		nodeShape       string
		storageSpec     *v1beta1.StorageSpec
		expectedResult  bool
		description     string
	}{
		{
			name:            "no annotations, empty storage spec",
			nodeAnnotations: map[string]string{},
			nodeShape:       "GPU.A10.2",
			storageSpec:     &v1beta1.StorageSpec{},
			expectedResult:  true,
			description:     "Should default to true when no annotations and empty storage spec",
		},
		{
			name: "shape in target shapes annotation",
			nodeAnnotations: map[string]string{
				constants.TargetInstanceShapes: "GPU.A10.2,GPU.A100.8",
			},
			nodeShape:      "GPU.A10.2",
			storageSpec:    &v1beta1.StorageSpec{},
			expectedResult: true,
			description:    "Should match when node shape is in target shapes annotation",
		},
		{
			name: "empty target shapes annotation",
			nodeAnnotations: map[string]string{
				constants.TargetInstanceShapes: "",
			},
			nodeShape:      "GPU.A10.2",
			storageSpec:    &v1beta1.StorageSpec{},
			expectedResult: true,
			description:    "Should default to true when target shapes annotation is empty",
		},
		{
			name: "annotation fallback not used with node selector",
			nodeAnnotations: map[string]string{
				constants.TargetInstanceShapes: "GPU.A100.8,GPU.H100.8", // Would not match
			},
			nodeShape: "GPU.A10.2",
			storageSpec: &v1beta1.StorageSpec{
				NodeSelector: map[string]string{
					"gpu-model": "a10", // This matches
				},
			},
			expectedResult: true,
			description:    "Should use node selector and not fallback to annotation",
		},
	}

	// Run tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up test node with specified annotations
			testNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-node",
					Labels: map[string]string{
						constants.NodeInstanceShapeLabel: tc.nodeShape,
						"gpu-model":                      "a10",
					},
					Annotations: tc.nodeAnnotations,
				},
			}

			// Create a test scount with the node
			scount := &Scout{
				nodeName: "test-node",
				nodeInfo: testNode,
				logger:   sugaredLogger,
			}

			result := scount.shouldDownloadModel(tc.storageSpec)
			if result != tc.expectedResult {
				t.Errorf("%s: Expected %v but got %v. %s", tc.name, tc.expectedResult, result, tc.description)
			}
		})
	}
}

// Test edge cases for the nodeMatchesExpression function
func TestNodeMatchesExpressionEdgeCases(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Set up test node
	testNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				"empty-value":     "",
				"null-like-value": "null",
				"boolean-true":    "true",
				"boolean-false":   "false",
			},
		},
	}

	// Create a test scount with minimal fields needed
	scount := &Scout{
		nodeName: "test-node",
		nodeInfo: testNode,
		logger:   sugaredLogger,
	}

	// Define test cases
	testCases := []struct {
		name           string
		expr           corev1.NodeSelectorRequirement
		expectedResult bool
		description    string
	}{
		{
			name: "empty value label with In operator",
			expr: corev1.NodeSelectorRequirement{
				Key:      "empty-value",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{""},
			},
			expectedResult: true,
			description:    "In operator should match when label has empty value and empty value is in values list",
		},
		{
			name: "empty value label with NotIn operator",
			expr: corev1.NodeSelectorRequirement{
				Key:      "empty-value",
				Operator: corev1.NodeSelectorOpNotIn,
				Values:   []string{"some-value"},
			},
			expectedResult: true,
			description:    "NotIn operator should match when label has empty value and empty value is not in values list",
		},
		{
			name: "null-like value with In operator",
			expr: corev1.NodeSelectorRequirement{
				Key:      "null-like-value",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"null"},
			},
			expectedResult: true,
			description:    "In operator should match when label has 'null' string value and 'null' is in values list",
		},
		{
			name: "boolean value matching",
			expr: corev1.NodeSelectorRequirement{
				Key:      "boolean-true",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"true"},
			},
			expectedResult: true,
			description:    "In operator should match when label has boolean-like string value",
		},
	}

	// Run tests
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := scount.nodeMatchesExpression(tc.expr)
			if result != tc.expectedResult {
				t.Errorf("%s: Expected %v but got %v. %s", tc.name, tc.expectedResult, result, tc.description)
			}
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}

func newClusterBaseModel(
	name string,
	policy v1beta1.DownloadPolicy,
	storageURI string,
) *v1beta1.ClusterBaseModel {
	return &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				DownloadPolicy: ptr(policy),
				StorageUri:     ptr(storageURI),
			},
		},
	}
}

func newBaseModel(
	name string,
	policy v1beta1.DownloadPolicy,
	storageURI string,
) *v1beta1.BaseModel {
	return &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				DownloadPolicy: ptr(policy),
				StorageUri:     ptr(storageURI),
			},
		},
	}
}

func TestDownloadPolicyOrDefault(t *testing.T) {
	tests := []struct {
		name     string
		storage  *v1beta1.StorageSpec
		expected v1beta1.DownloadPolicy
	}{
		{
			name:     "storage is nil -> default AlwaysDownload",
			storage:  nil,
			expected: v1beta1.AlwaysDownload,
		},
		{
			name: "storage exists but DownloadPolicy is nil -> default AlwaysDownload",
			storage: &v1beta1.StorageSpec{
				DownloadPolicy: nil,
			},
			expected: v1beta1.AlwaysDownload,
		},
		{
			name: "storage exists and DownloadPolicy is set",
			storage: &v1beta1.StorageSpec{
				DownloadPolicy: ptr(v1beta1.ReuseIfExists),
			},
			expected: v1beta1.ReuseIfExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := downloadPolicyOrDefault(tt.storage)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScout_isToDownloadOverrideDueToDownloadPolicy_CBM(t *testing.T) {
	tests := []struct {
		name     string
		oldModel *v1beta1.ClusterBaseModel
		newModel *v1beta1.ClusterBaseModel
		expected bool
	}{
		{
			name: "download policy changed and storage is HuggingFace",
			oldModel: newClusterBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			newModel: newClusterBaseModel(
				"new-model",
				v1beta1.AlwaysDownload,
				"hf://meta-llama/llama-3",
			),
			expected: true,
		},
		{
			name: "download policy unchanged and storage is HuggingFace",
			oldModel: newClusterBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			newModel: newClusterBaseModel(
				"new-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			expected: false,
		},
		{
			name: "download policy changed but storage is not HuggingFace",
			oldModel: newClusterBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"s3://bucket/model",
			),
			newModel: newClusterBaseModel(
				"new-model",
				v1beta1.AlwaysDownload,
				"s3://bucket/model",
			),
			expected: false,
		},
		{
			name: "download policy unchanged and storage is not HuggingFace",
			oldModel: newClusterBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"s3://bucket/model",
			),
			newModel: newClusterBaseModel(
				"new-model",
				v1beta1.ReuseIfExists,
				"s3://bucket/model",
			),
			expected: false,
		},
		{
			name: "invalid storage URI returns false",
			oldModel: newClusterBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			newModel: newClusterBaseModel(
				"new-model",
				v1beta1.AlwaysDownload,
				"://invalid-uri",
			),
			expected: false,
		},
	}

	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)
	scout := &Scout{
		logger: sugaredLogger,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scout.isToDownloadOverrideDueToDownloadPolicyBasedOnCBM(
				tt.oldModel,
				tt.newModel,
			)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScout_isToDownloadOverrideDueToDownloadPolicy_BM(t *testing.T) {
	tests := []struct {
		name     string
		oldModel *v1beta1.BaseModel
		newModel *v1beta1.BaseModel
		expected bool
	}{
		{
			name: "download policy changed and storage is HuggingFace",
			oldModel: newBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			newModel: newBaseModel(
				"new-model",
				v1beta1.AlwaysDownload,
				"hf://meta-llama/llama-3",
			),
			expected: true,
		},
		{
			name: "download policy unchanged and storage is HuggingFace",
			oldModel: newBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			newModel: newBaseModel(
				"new-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			expected: false,
		},
		{
			name: "download policy changed but storage is not HuggingFace",
			oldModel: newBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"s3://bucket/model",
			),
			newModel: newBaseModel(
				"new-model",
				v1beta1.AlwaysDownload,
				"s3://bucket/model",
			),
			expected: false,
		},
		{
			name: "download policy unchanged and storage is not HuggingFace",
			oldModel: newBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"s3://bucket/model",
			),
			newModel: newBaseModel(
				"new-model",
				v1beta1.ReuseIfExists,
				"s3://bucket/model",
			),
			expected: false,
		},
		{
			name: "invalid storage URI returns false",
			oldModel: newBaseModel(
				"old-model",
				v1beta1.ReuseIfExists,
				"hf://meta-llama/llama-3",
			),
			newModel: newBaseModel(
				"new-model",
				v1beta1.AlwaysDownload,
				"://invalid-uri",
			),
			expected: false,
		},
	}

	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)
	scout := &Scout{
		logger: sugaredLogger,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scout.isToDownloadOverrideDueToDownloadPolicyBasedOnBM(
				tt.oldModel,
				tt.newModel,
			)

			assert.Equal(t, tt.expected, result)
		})
	}
}

// Tests for generateDownloadOverrideTaskBasedOnClusterBaseModel
func TestGenerateDownloadOverrideTaskBasedOnClusterBaseModel_Defaults(t *testing.T) {
	// logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(s *zap.SugaredLogger) { _ = s.Sync() }(sugaredLogger)

	// setup
	ch := make(chan *GopherTask, 1)
	scout := &Scout{
		nodeShapeAlias: "a10",
		gopherChan:     ch,
		logger:         sugaredLogger,
	}

	cbm := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cbm-default",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat:        v1beta1.ModelFormat{Name: "onnx"},
			AdditionalMetadata: map[string]string{},
		},
	}

	// act
	scout.generateDownloadOverrideTaskBasedOnClusterBaseModel(cbm)

	// assert
	task := <-ch
	assert.NotNil(t, task)
	assert.Equal(t, DownloadOverride, task.TaskType)
	assert.Equal(t, cbm, task.ClusterBaseModel)
	assert.Nil(t, task.BaseModel)
	assert.NotNil(t, task.TensorRTLLMShapeFilter)

	filter := task.TensorRTLLMShapeFilter
	assert.False(t, filter.IsTensorrtLLMModel)
	assert.Equal(t, "a10", filter.ShapeAlias)
	assert.Equal(t, string(constants.ServingBaseModel), filter.ModelType)
}

func TestGenerateDownloadOverrideTaskBasedOnClusterBaseModel_TensorRTLLMAndMetadataType(t *testing.T) {
	// logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(s *zap.SugaredLogger) { _ = s.Sync() }(sugaredLogger)

	// setup
	ch := make(chan *GopherTask, 1)
	scout := &Scout{
		nodeShapeAlias: "a10",
		gopherChan:     ch,
		logger:         sugaredLogger,
	}

	cbm := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cbm-trtllm",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{Name: constants.TensorRTLLM},
			AdditionalMetadata: map[string]string{
				"type": "CustomType",
			},
		},
	}

	// act
	scout.generateDownloadOverrideTaskBasedOnClusterBaseModel(cbm)

	// assert
	task := <-ch
	assert.NotNil(t, task)
	assert.Equal(t, DownloadOverride, task.TaskType)
	assert.Equal(t, cbm, task.ClusterBaseModel)
	assert.Nil(t, task.BaseModel)
	assert.NotNil(t, task.TensorRTLLMShapeFilter)

	filter := task.TensorRTLLMShapeFilter
	assert.True(t, filter.IsTensorrtLLMModel)
	assert.Equal(t, "a10", filter.ShapeAlias)
	assert.Equal(t, "CustomType", filter.ModelType)
}

func TestGenerateDownloadOverrideTaskBasedOnBaseModel_Defaults(t *testing.T) {
	// logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(s *zap.SugaredLogger) { _ = s.Sync() }(sugaredLogger)

	// setup
	ch := make(chan *GopherTask, 1)
	scout := &Scout{
		nodeShapeAlias: "a10",
		gopherChan:     ch,
		logger:         sugaredLogger,
	}

	bm := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cbm-default",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat:        v1beta1.ModelFormat{Name: "onnx"},
			AdditionalMetadata: map[string]string{},
		},
	}

	// act
	scout.generateDownloadOverrideTaskBasedOnBaseModel(bm)

	// assert
	task := <-ch
	assert.NotNil(t, task)
	assert.Equal(t, DownloadOverride, task.TaskType)
	assert.Equal(t, bm, task.BaseModel)
	assert.Nil(t, task.ClusterBaseModel)
	assert.NotNil(t, task.TensorRTLLMShapeFilter)

	filter := task.TensorRTLLMShapeFilter
	assert.False(t, filter.IsTensorrtLLMModel)
	assert.Equal(t, "a10", filter.ShapeAlias)
	assert.Equal(t, string(constants.ServingBaseModel), filter.ModelType)
}

func TestGenerateDownloadOverrideTaskBasedOnBaseModel_TensorRTLLMAndMetadataType(t *testing.T) {
	// logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(s *zap.SugaredLogger) { _ = s.Sync() }(sugaredLogger)

	// setup
	ch := make(chan *GopherTask, 1)
	scout := &Scout{
		nodeShapeAlias: "a10",
		gopherChan:     ch,
		logger:         sugaredLogger,
	}

	bm := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cbm-trtllm",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{Name: constants.TensorRTLLM},
			AdditionalMetadata: map[string]string{
				"type": "CustomType",
			},
		},
	}

	// act
	scout.generateDownloadOverrideTaskBasedOnBaseModel(bm)

	// assert
	task := <-ch
	assert.NotNil(t, task)
	assert.Equal(t, DownloadOverride, task.TaskType)
	assert.Equal(t, bm, task.BaseModel)
	assert.Nil(t, task.ClusterBaseModel)
	assert.NotNil(t, task.TensorRTLLMShapeFilter)

	filter := task.TensorRTLLMShapeFilter
	assert.True(t, filter.IsTensorrtLLMModel)
	assert.Equal(t, "a10", filter.ShapeAlias)
	assert.Equal(t, "CustomType", filter.ModelType)
}

func TestUpdateBaseModel_NoDuplicateTaskOnPolicyOnlyChange(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(s *zap.SugaredLogger) { _ = s.Sync() }(sugaredLogger)

	ch := make(chan *GopherTask, 10)
	scout := &Scout{
		nodeShapeAlias: "a10",
		gopherChan:     ch,
		logger:         sugaredLogger,
		nodeInfo: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{"gpu-model": "a10"},
			},
		},
	}

	tests := []struct {
		name          string
		oldModel      *v1beta1.BaseModel
		newModel      *v1beta1.BaseModel
		expectedTasks int
		description   string
	}{
		{
			name:          "policy-only change on HuggingFace - exactly 1 task from dedicated handler",
			oldModel:      newBaseModel("model-1", v1beta1.ReuseIfExists, "hf://meta-llama/llama-3"),
			newModel:      newBaseModel("model-1", v1beta1.AlwaysDownload, "hf://meta-llama/llama-3"),
			expectedTasks: 1,
			description:   "Only the dedicated policy-change handler should fire",
		},
		{
			name:          "policy-only change on non-HuggingFace - 0 tasks",
			oldModel:      newBaseModel("model-2", v1beta1.ReuseIfExists, "oci://bucket/model"),
			newModel:      newBaseModel("model-2", v1beta1.AlwaysDownload, "oci://bucket/model"),
			expectedTasks: 0,
			description:   "Dedicated handler skips non-HF, spec diff excludes DownloadPolicy",
		},
		{
			name: "non-policy spec change - exactly 1 task from hasChanges block",
			oldModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model-3"},
				Spec: v1beta1.BaseModelSpec{
					ModelExtensionSpec: v1beta1.ModelExtensionSpec{DisplayName: ptr("Old Name")},
					Storage: &v1beta1.StorageSpec{
						DownloadPolicy: ptr(v1beta1.AlwaysDownload),
						StorageUri:     ptr("hf://meta-llama/llama-3"),
					},
				},
			},
			newModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model-3"},
				Spec: v1beta1.BaseModelSpec{
					ModelExtensionSpec: v1beta1.ModelExtensionSpec{DisplayName: ptr("New Name")},
					Storage: &v1beta1.StorageSpec{
						DownloadPolicy: ptr(v1beta1.AlwaysDownload),
						StorageUri:     ptr("hf://meta-llama/llama-3"),
					},
				},
			},
			expectedTasks: 1,
			description:   "Only the hasChanges block should fire",
		},
		{
			name: "policy change + other spec change on HuggingFace - exactly 1 task",
			oldModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model-4"},
				Spec: v1beta1.BaseModelSpec{
					ModelExtensionSpec: v1beta1.ModelExtensionSpec{DisplayName: ptr("Old Name")},
					Storage: &v1beta1.StorageSpec{
						DownloadPolicy: ptr(v1beta1.ReuseIfExists),
						StorageUri:     ptr("hf://meta-llama/llama-3"),
					},
				},
			},
			newModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model-4"},
				Spec: v1beta1.BaseModelSpec{
					ModelExtensionSpec: v1beta1.ModelExtensionSpec{DisplayName: ptr("New Name")},
					Storage: &v1beta1.StorageSpec{
						DownloadPolicy: ptr(v1beta1.AlwaysDownload),
						StorageUri:     ptr("hf://meta-llama/llama-3"),
					},
				},
			},
			expectedTasks: 1,
			description:   "Single task even when both policy and spec change",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scout.updateBaseModel(tc.oldModel, tc.newModel)
			assert.Equal(t, tc.expectedTasks, len(ch), tc.description)
			for range tc.expectedTasks {
				task := <-ch
				assert.Equal(t, DownloadOverride, task.TaskType)
			}
		})
	}
}

func TestUpdateClusterBaseModel_NoDuplicateTaskOnPolicyOnlyChange(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(s *zap.SugaredLogger) { _ = s.Sync() }(sugaredLogger)

	ch := make(chan *GopherTask, 10)
	scout := &Scout{
		nodeShapeAlias: "a10",
		gopherChan:     ch,
		logger:         sugaredLogger,
		nodeInfo: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{"gpu-model": "a10"},
			},
		},
	}

	tests := []struct {
		name          string
		oldModel      *v1beta1.ClusterBaseModel
		newModel      *v1beta1.ClusterBaseModel
		expectedTasks int
		description   string
	}{
		{
			name:          "policy-only change on HuggingFace - exactly 1 task",
			oldModel:      newClusterBaseModel("cbm-1", v1beta1.ReuseIfExists, "hf://meta-llama/llama-3"),
			newModel:      newClusterBaseModel("cbm-1", v1beta1.AlwaysDownload, "hf://meta-llama/llama-3"),
			expectedTasks: 1,
			description:   "Only the dedicated policy-change handler should fire",
		},
		{
			name:          "policy-only change on non-HuggingFace - 0 tasks",
			oldModel:      newClusterBaseModel("cbm-2", v1beta1.ReuseIfExists, "oci://bucket/model"),
			newModel:      newClusterBaseModel("cbm-2", v1beta1.AlwaysDownload, "oci://bucket/model"),
			expectedTasks: 0,
			description:   "Dedicated handler skips non-HF, spec diff excludes DownloadPolicy",
		},
		{
			name: "non-policy spec change - exactly 1 task",
			oldModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "cbm-3"},
				Spec: v1beta1.BaseModelSpec{
					ModelExtensionSpec: v1beta1.ModelExtensionSpec{DisplayName: ptr("Old Name")},
					Storage: &v1beta1.StorageSpec{
						DownloadPolicy: ptr(v1beta1.AlwaysDownload),
						StorageUri:     ptr("hf://meta-llama/llama-3"),
					},
				},
			},
			newModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "cbm-3"},
				Spec: v1beta1.BaseModelSpec{
					ModelExtensionSpec: v1beta1.ModelExtensionSpec{DisplayName: ptr("New Name")},
					Storage: &v1beta1.StorageSpec{
						DownloadPolicy: ptr(v1beta1.AlwaysDownload),
						StorageUri:     ptr("hf://meta-llama/llama-3"),
					},
				},
			},
			expectedTasks: 1,
			description:   "Only the hasChanges block should fire",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scout.updateClusterBaseModel(tc.oldModel, tc.newModel)
			assert.Equal(t, tc.expectedTasks, len(ch), tc.description)
			for range tc.expectedTasks {
				task := <-ch
				assert.Equal(t, DownloadOverride, task.TaskType)
			}
		})
	}
}
