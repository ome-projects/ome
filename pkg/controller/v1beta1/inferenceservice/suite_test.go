package inferenceservice

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// Global test variables
var (
	k8sClient client.Client
	testCtx   context.Context
	cancel    context.CancelFunc
)

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "InferenceService Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("Setting up test context")
	testCtx, cancel = context.WithCancel(context.TODO())

	By("Creating fake client")
	// Create initial objects for the fake client
	initialObjects := []client.Object{
		// Add the inferenceservice-config ConfigMap
		&v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "inferenceservice-config",
				Namespace: "ome",
			},
			Data: map[string]string{
				"deploy": `{"defaultDeploymentMode": "RawDeployment"}`,
			},
		},
		// Add a test namespace
		&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "default",
			},
		},
	}

	k8sClient = fakeclient.New(GinkgoTB(),
		fakeclient.WithTypes(kedav1.AddToScheme, lws.AddToScheme, monitoringv1.AddToScheme),
		fakeclient.WithObjects(initialObjects...),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceService{}))

	// Create test data in the client
	Expect(createTestData()).To(Succeed())
})

var _ = AfterSuite(func() {
	By("Tearing down the test environment")
	cancel()
})

// Helper functions for creating test data
func createTestData() error {
	ctx := context.Background()

	// Create ServingRuntimes
	llmRuntime := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llm-runtime",
			Namespace: "default",
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{
				{
					Name:       "safetensors",
					Version:    stringPtr("*"),
					AutoSelect: boolPtr(true),
					ModelFormat: &v1beta1.ModelFormat{
						Name: "safetensors",
					},
				},
			},
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				Containers: []v1.Container{
					{
						Name:  "ome-container",
						Image: "llm-server:latest",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
							},
						},
					},
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, llmRuntime); err != nil {
		return err
	}

	// Create BaseModels
	baseModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-7b",
			Namespace: "default",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{
				Name:    "safetensors",
				Version: stringPtr("1.0"),
			},
			ModelParameterSize: stringPtr("7B"),
			Storage: &v1beta1.StorageSpec{
				Path: stringPtr("/models/llama-7b"),
			},
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Disabled: boolPtr(false),
			},
		},
	}
	if err := k8sClient.Create(ctx, baseModel); err != nil {
		return err
	}

	// Create FineTunedWeight for fine-tuned model tests
	fineTunedWeight := &v1beta1.FineTunedWeight{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-7b-finetuned",
			Namespace: "default",
		},
		Spec: v1beta1.FineTunedWeightSpec{
			BaseModelRef: v1beta1.ObjectReference{
				Name: stringPtr("llama-7b"),
			},
			Storage: &v1beta1.StorageSpec{
				Path: stringPtr("/models/llama-7b-finetuned"),
			},
			ModelType: stringPtr("LoRA"),
			HyperParameters: runtime.RawExtension{
				Raw: []byte(`{"rank": 8, "alpha": 16}`),
			},
		},
	}
	if err := k8sClient.Create(ctx, fineTunedWeight); err != nil {
		return err
	}

	// Create additional base model for unsupported format test
	unsupportedModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unsupported-model",
			Namespace: "default",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{
				Name:    "unknown-format",
				Version: stringPtr("1.0"),
			},
			ModelParameterSize: stringPtr("1B"),
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{
				Disabled: boolPtr(false),
			},
		},
	}
	if err := k8sClient.Create(ctx, unsupportedModel); err != nil {
		return err
	}

	return nil
}

// Helper functions
func intPtr(i int) *int {
	return &i
}

func stringPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}
