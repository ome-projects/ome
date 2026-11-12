package benchmark

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestValidateBenchmarkJob(t *testing.T) {
	scenarios := map[string]struct {
		benchmarkJob *v1beta1.BenchmarkJob
		expected     gomega.OmegaMatcher
	}{
		"Valid OpenAI text-to-text benchmark job": {
			benchmarkJob: &v1beta1.BenchmarkJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-benchmark",
					Namespace: "default",
				},
				Spec: v1beta1.BenchmarkJobSpec{
					Task: "text-to-text",
					Endpoint: v1beta1.EndpointSpec{
						Endpoint: &v1beta1.Endpoint{
							URL: "https://api.openai.com/v1/chat/completions",
						},
					},
					TrafficScenarios: []string{"N(10,20)/(30,40)"},
					AdditionalRequestParams: map[string]string{
						"temperature": "0.7",
						"max_tokens":  "100",
					},
				},
			},
			expected: gomega.BeNil(),
		},
		"Missing endpoint and inference service": {
			benchmarkJob: &v1beta1.BenchmarkJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "missing-endpoint",
					Namespace: "default",
				},
				Spec: v1beta1.BenchmarkJobSpec{
					Task: "text-to-text",
				},
			},
			expected: gomega.HaveOccurred(),
		},
		"Both endpoint and inference service specified": {
			benchmarkJob: &v1beta1.BenchmarkJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "both-endpoints",
					Namespace: "default",
				},
				Spec: v1beta1.BenchmarkJobSpec{
					Task: "text-to-text",
					Endpoint: v1beta1.EndpointSpec{
						Endpoint: &v1beta1.Endpoint{
							URL: "https://api.openai.com/v1/chat/completions",
						},
						InferenceService: &v1beta1.InferenceServiceReference{
							Name: "test-service",
						},
					},
				},
			},
			expected: gomega.HaveOccurred(),
		},
		"Invalid traffic scenario format": {
			benchmarkJob: &v1beta1.BenchmarkJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-scenario",
					Namespace: "default",
				},
				Spec: v1beta1.BenchmarkJobSpec{
					Task: "text-to-text",
					Endpoint: v1beta1.EndpointSpec{
						Endpoint: &v1beta1.Endpoint{
							URL: "https://api.openai.com/v1/chat/completions",
						},
					},
					TrafficScenarios: []string{"invalid-format"},
				},
			},
			expected: gomega.HaveOccurred(),
		},
		"Invalid additional request parameters": {
			benchmarkJob: &v1beta1.BenchmarkJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-params",
					Namespace: "default",
				},
				Spec: v1beta1.BenchmarkJobSpec{
					Task: "text-to-text",
					Endpoint: v1beta1.EndpointSpec{
						Endpoint: &v1beta1.Endpoint{
							URL: "https://api.openai.com/v1/chat/completions",
						},
					},
					AdditionalRequestParams: map[string]string{
						"temperature": "invalid",
					},
				},
			},
			expected: gomega.HaveOccurred(),
		},
	}

	g := gomega.NewGomegaWithT(t)
	validator := &BenchmarkJobValidator{}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			err := validator.validateBenchmarkJob(context.Background(), scenario.benchmarkJob)
			g.Expect(err).To(scenario.expected)
		})
	}
}

func TestValidateEndpoint(t *testing.T) {
	scenarios := map[string]struct {
		endpoint v1beta1.EndpointSpec
		expected gomega.OmegaMatcher
	}{
		"Valid endpoint URL": {
			endpoint: v1beta1.EndpointSpec{
				Endpoint: &v1beta1.Endpoint{
					URL: "https://api.openai.com/v1/chat/completions",
				},
			},
			expected: gomega.BeNil(),
		},
		"Valid inference service": {
			endpoint: v1beta1.EndpointSpec{
				InferenceService: &v1beta1.InferenceServiceReference{
					Name: "test-service",
				},
			},
			expected: gomega.BeNil(),
		},
		"Missing both endpoint and inference service": {
			endpoint: v1beta1.EndpointSpec{},
			expected: gomega.HaveOccurred(),
		},
		"Both endpoint and inference service specified": {
			endpoint: v1beta1.EndpointSpec{
				Endpoint: &v1beta1.Endpoint{
					URL: "https://api.openai.com/v1/chat/completions",
				},
				InferenceService: &v1beta1.InferenceServiceReference{
					Name: "test-service",
				},
			},
			expected: gomega.HaveOccurred(),
		},
	}

	g := gomega.NewGomegaWithT(t)
	validator := &BenchmarkJobValidator{}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			err := validator.validateEndpoint(scenario.endpoint)
			g.Expect(err).To(scenario.expected)
		})
	}
}

func TestValidateTrafficScenarios(t *testing.T) {
	scenarios := map[string]struct {
		task      string
		scenarios []string
		expected  gomega.OmegaMatcher
	}{
		"Valid text-to-text normal scenario": {
			task:      "text-to-text",
			scenarios: []string{"N(10,20)/(30,40)"},
			expected:  gomega.BeNil(),
		},
		"Valid text-to-text uniform scenario": {
			task:      "text-to-text",
			scenarios: []string{"U(10,20)"},
			expected:  gomega.BeNil(),
		},
		"Valid text-to-embeddings scenario": {
			task:      "text-to-embeddings",
			scenarios: []string{"E(10,20)"},
			expected:  gomega.BeNil(),
		},
		"Invalid scenario format": {
			task:      "text-to-text",
			scenarios: []string{"invalid-format"},
			expected:  gomega.HaveOccurred(),
		},
		"Invalid scenario for task type": {
			task:      "text-to-embeddings",
			scenarios: []string{"N10,20/30,40"},
			expected:  gomega.HaveOccurred(),
		},
	}

	g := gomega.NewGomegaWithT(t)
	validator := &BenchmarkJobValidator{}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			err := validator.validateTrafficScenarios(scenario.task, scenario.scenarios)
			g.Expect(err).To(scenario.expected)
		})
	}
}

func TestValidateAdditionalRequestParams(t *testing.T) {
	scenarios := map[string]struct {
		params   map[string]string
		expected gomega.OmegaMatcher
	}{
		"Valid parameters": {
			params: map[string]string{
				"temperature": "0.7",
				"max_tokens":  "100",
			},
			expected: gomega.BeNil(),
		},
		"Invalid temperature value": {
			params: map[string]string{
				"temperature": "invalid",
			},
			expected: gomega.HaveOccurred(),
		},
		"Temperature out of range": {
			params: map[string]string{
				"temperature": "2.0",
			},
			expected: gomega.BeNil(),
		},
	}

	g := gomega.NewGomegaWithT(t)
	validator := &BenchmarkJobValidator{}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			err := validator.validateAdditionalRequestParams(scenario.params)
			g.Expect(err).To(scenario.expected)
		})
	}
}

func TestValidateStorage(t *testing.T) {
	scenarios := map[string]struct {
		storage  *v1beta1.StorageSpec
		expected gomega.OmegaMatcher
	}{
		"Valid storage URI": {
			storage: &v1beta1.StorageSpec{
				StorageUri: ptr("oci://n/mynamespace/b/mybucket/o/path/to/object"),
			},
			expected: gomega.BeNil(),
		},
		"Nil storage": {
			storage:  nil,
			expected: gomega.BeNil(),
		},
		"Nil storage URI": {
			storage: &v1beta1.StorageSpec{
				StorageUri: nil,
			},
			expected: gomega.HaveOccurred(),
		},
		"Short storage URI form - bucket and prefix": {
			storage: &v1beta1.StorageSpec{
				StorageUri: ptr("oci://mynamespace/b/mybucket/o/object"),
			},
			expected: gomega.Succeed(),
		},
		"Invalid storage URI format - missing b": {
			storage: &v1beta1.StorageSpec{
				StorageUri: ptr("oci://n/mynamespace/mybucket/o/object"),
			},
			expected: gomega.HaveOccurred(),
		},
		"Invalid storage URI format - missing o": {
			storage: &v1beta1.StorageSpec{
				StorageUri: ptr("oci://n/mynamespace/b/mybucket/object"),
			},
			expected: gomega.HaveOccurred(),
		},
	}

	g := gomega.NewGomegaWithT(t)
	validator := &BenchmarkJobValidator{}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			err := validator.validateStorage(scenario.storage)
			g.Expect(err).To(scenario.expected)
		})
	}
}

// ptr returns a pointer to the string value
func ptr(s string) *string {
	return &s
}
