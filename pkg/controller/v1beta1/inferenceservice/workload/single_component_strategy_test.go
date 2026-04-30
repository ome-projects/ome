package workload

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/components"
	omeTesting "github.com/sgl-project/ome/pkg/testing"
)

// Helper function to create a basic test InferenceService.
func createTestInferenceService() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			UID:       "test-uid",
		},
		Spec: v1beta1.InferenceServiceSpec{},
	}
}

// TestGetStrategyName tests getting the strategy name.
func TestGetStrategyName(t *testing.T) {
	log := logr.Discard()
	strategy := NewSingleComponentStrategy(log)

	name := strategy.GetStrategyName()
	assert.Equal(t, "SingleComponent", name)
}

func TestIsApplicable_AlwaysTrue(t *testing.T) {
	log := logr.Discard()
	strategy := NewSingleComponentStrategy(log)

	testCases := []struct {
		isvc           *v1beta1.InferenceService
		deploymentMode constants.DeploymentModeType
	}{
		{
			isvc:           createTestInferenceService(),
			deploymentMode: constants.RawDeployment,
		},
		{
			isvc:           createTestInferenceService(),
			deploymentMode: constants.MultiNode,
		},
		{
			isvc:           createTestInferenceService(),
			deploymentMode: constants.Serverless,
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			applicable := strategy.IsApplicable(tc.isvc, tc.deploymentMode)
			assert.True(t, applicable, "SingleComponent strategy should always be applicable")
		})
	}
}

// TestValidateDeploymentModes tests component deployment mode validation.
func TestValidateDeploymentModes(t *testing.T) {
	log := logr.Discard()
	strategy := NewSingleComponentStrategy(log)

	testCases := []struct {
		name  string
		modes *ComponentDeploymentModes
	}{
		{
			name: "all raw deployment",
			modes: &ComponentDeploymentModes{
				Engine:  constants.RawDeployment,
				Decoder: constants.RawDeployment,
				Router:  constants.RawDeployment,
			},
		},
		{
			name: "mixed deployment modes",
			modes: &ComponentDeploymentModes{
				Engine:  constants.RawDeployment,
				Decoder: constants.MultiNode,
				Router:  constants.Serverless,
			},
		},
		{
			name:  "nil modes",
			modes: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := strategy.ValidateDeploymentModes(tc.modes)
			assert.NoError(t, err, "SingleComponent strategy should support all deployment modes")
		})
	}
}

// TestSingleComponentStrategyReconcileWorkload tests the ReconcileWorkload method with integration setup.
func TestSingleComponentStrategyReconcileWorkload(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup EnvTest environment
	testEnv := omeTesting.SetupEnvTest()
	cfg, err := testEnv.Start()
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(cfg).NotTo(gomega.BeNil())
	defer func(testEnv *envtest.Environment) {
		_ = testEnv.Stop()
	}(testEnv)

	// Create scheme
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(appsv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(lws.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(autoscalingv2.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(policyv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(rbacv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name         string
		isvc         *v1beta1.InferenceService
		setupMocks   func(client.Client, *fake.Clientset)
		buildRequest func(client.Client, *fake.Clientset, *v1beta1.InferenceService) *WorkloadReconcileRequest
		validate     func(*testing.T, client.Client, string)
	}{
		{
			name: "New architecture with engine only",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-engine-only",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "base-model-1",
						Kind: stringPtr("BaseModel"),
					},
					Engine: &v1beta1.EngineSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "ome-container",
									Image: "engine:latest",
								},
							},
						},
					},
				},
			},
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				setupCommonMocks(g, c, cs, "base-model-1")

				// Create ServingRuntime
				rt := &v1beta1.ServingRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-runtime-1",
						Namespace: "default",
					},
					Spec: v1beta1.ServingRuntimeSpec{
						SupportedModelFormats: []v1beta1.SupportedModelFormat{
							{
								Name:       "safetensors",
								Version:    stringPtr("*"),
								AutoSelect: boolPtr(true),
								ModelFormat: &v1beta1.ModelFormat{
									Name:    "safetensors",
									Version: stringPtr("1.0.0"),
									Weight:  int64(1),
								},
							},
						},
						ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
							Containers: []v1.Container{
								{
									Name:  "ome-container",
									Image: "runtime:v1",
								},
							},
						},
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			buildRequest: func(c client.Client, cs *fake.Clientset, isvc *v1beta1.InferenceService) *WorkloadReconcileRequest {
				// Get BaseModel
				baseModel := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{Name: "base-model-1", Namespace: "default"}, baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Get ServingRuntime
				rt := &v1beta1.ServingRuntime{}
				err = c.Get(context.TODO(), types.NamespacedName{Name: "test-runtime-1", Namespace: "default"}, rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create InferenceServiceConfig
				isvcConfig, err := controllerconfig.NewInferenceServicesConfig(cs)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create ComponentBuilderFactory
				factory := components.NewComponentBuilderFactory(c, cs, c.Scheme(), isvcConfig)

				return &WorkloadReconcileRequest{
					InferenceService: isvc,
					BaseModel:        &baseModel.Spec,
					BaseModelMeta:    &baseModel.ObjectMeta,
					Runtime:          &rt.Spec,
					RuntimeName:      rt.Name,
					MergedEngine: &v1beta1.EngineSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "engine",
									Image: "engine:latest",
								},
							},
						},
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 3,
						},
					},
					MergedDecoder: nil,
					MergedRouter:  nil,
					DeploymentModes: &ComponentDeploymentModes{
						Engine:  constants.RawDeployment,
						Decoder: "",
						Router:  "",
					},
					ComponentBuilderFactory:     factory,
					UserSpecifiedRuntime:        false,
					EngineSupportedModelFormat:  &rt.Spec.SupportedModelFormats[0],
					DecoderSupportedModelFormat: nil,
				}
			},
			validate: func(t *testing.T, c client.Client, isvcName string) {
				// Verify Engine Deployment
				engineDeployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-engine",
					Namespace: "default",
				}, engineDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(engineDeployment.Spec.Template.Spec.Containers[0].Image).To(gomega.Equal("engine:latest"))

				// Verify Engine Service
				engineService := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-engine",
					Namespace: "default",
				}, engineService)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Engine HPA
				engineHPA := &autoscalingv2.HorizontalPodAutoscaler{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-engine",
					Namespace: "default",
				}, engineHPA)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Engine PDB
				enginePDB := &policyv1.PodDisruptionBudget{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-engine",
					Namespace: "default",
				}, enginePDB)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Decoder resources do NOT exist
				decoderDeployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-decoder",
					Namespace: "default",
				}, decoderDeployment)
				g.Expect(err).To(gomega.HaveOccurred())

				// Verify Router resources do NOT exist
				routerDeployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-router",
					Namespace: "default",
				}, routerDeployment)
				g.Expect(err).To(gomega.HaveOccurred())
			},
		},
		{
			name: "New architecture with engine and decoder (PD-disaggregated)",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pd-disaggregated",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "base-model-2",
						Kind: stringPtr("BaseModel"),
					},
					Runtime: &v1beta1.ServingRuntimeRef{
						Name: "custom-runtime",
					},
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
				},
			},
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				setupCommonMocks(g, c, cs, "base-model-2")

				// Create ServingRuntime
				rt := &v1beta1.ServingRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "custom-runtime",
						Namespace: "default",
					},
					Spec: v1beta1.ServingRuntimeSpec{
						SupportedModelFormats: []v1beta1.SupportedModelFormat{
							{
								Name:       "safetensors",
								Version:    stringPtr("*"),
								AutoSelect: boolPtr(true),
								ModelFormat: &v1beta1.ModelFormat{
									Name:    "safetensors",
									Version: stringPtr("1.0.0"),
									Weight:  int64(1),
								},
							},
						},
						EngineConfig: &v1beta1.EngineSpec{
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ome-container",
										Image: "runtime:v1",
									},
								},
							},
						},
						DecoderConfig: &v1beta1.DecoderSpec{
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ome-container",
										Image: "runtime:v1",
									},
								},
							},
						},
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			buildRequest: func(c client.Client, cs *fake.Clientset, isvc *v1beta1.InferenceService) *WorkloadReconcileRequest {
				// Get BaseModel
				baseModel := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{Name: "base-model-2", Namespace: "default"}, baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Get ServingRuntime
				rt := &v1beta1.ServingRuntime{}
				err = c.Get(context.TODO(), types.NamespacedName{Name: "custom-runtime", Namespace: "default"}, rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create InferenceServiceConfig
				isvcConfig, err := controllerconfig.NewInferenceServicesConfig(cs)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create ComponentBuilderFactory
				factory := components.NewComponentBuilderFactory(c, cs, c.Scheme(), isvcConfig)

				return &WorkloadReconcileRequest{
					InferenceService: isvc,
					BaseModel:        &baseModel.Spec,
					BaseModelMeta:    &baseModel.ObjectMeta,
					Runtime:          &rt.Spec,
					RuntimeName:      rt.Name,
					MergedEngine: &v1beta1.EngineSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "ome-container",
									Image: "runtime:v1",
								},
							},
						},
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					MergedDecoder: &v1beta1.DecoderSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "ome-container",
									Image: "runtime:v1",
								},
							},
						},
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					MergedRouter: nil,
					DeploymentModes: &ComponentDeploymentModes{
						Engine:  constants.RawDeployment,
						Decoder: constants.RawDeployment,
						Router:  "",
					},
					ComponentBuilderFactory:     factory,
					UserSpecifiedRuntime:        true,
					EngineSupportedModelFormat:  &rt.Spec.SupportedModelFormats[0],
					DecoderSupportedModelFormat: &rt.Spec.SupportedModelFormats[0],
				}
			},
			validate: func(t *testing.T, c client.Client, isvcName string) {
				// Verify Engine resources
				engineDeployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-engine",
					Namespace: "default",
				}, engineDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				engineService := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-engine",
					Namespace: "default",
				}, engineService)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Decoder resources
				decoderDeployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-decoder",
					Namespace: "default",
				}, decoderDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				decoderService := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-decoder",
					Namespace: "default",
				}, decoderService)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Router resources do NOT exist
				routerDeployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-router",
					Namespace: "default",
				}, routerDeployment)
				g.Expect(err).To(gomega.HaveOccurred())
			},
		},
		{
			name: "Multi-node && specified runtime && PD-disaggregated && router",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-multinode",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "base-model-4",
						Kind: stringPtr("BaseModel"),
					},
					Runtime: &v1beta1.ServingRuntimeRef{
						Name: "custom-runtime",
					},
					Engine: &v1beta1.EngineSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					Decoder: &v1beta1.DecoderSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					Router: &v1beta1.RouterSpec{
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
				},
			},
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				setupCommonMocks(g, c, cs, "base-model-4")
				// Create ServingRuntime
				rt := &v1beta1.ServingRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "custom-runtime",
						Namespace: "default",
					},
					Spec: v1beta1.ServingRuntimeSpec{
						SupportedModelFormats: []v1beta1.SupportedModelFormat{
							{
								Name:       "safetensors",
								Version:    stringPtr("*"),
								AutoSelect: boolPtr(true),
								ModelFormat: &v1beta1.ModelFormat{
									Name:    "safetensors",
									Version: stringPtr("1.0.0"),
									Weight:  int64(1),
								},
							},
						},
						EngineConfig: &v1beta1.EngineSpec{
							Leader: &v1beta1.LeaderSpec{
								PodSpec: v1beta1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "ome-container",
											Image: "runtime:v1",
										},
									},
								},
							},
							Worker: &v1beta1.WorkerSpec{
								Size: intPtr(2),
								PodSpec: v1beta1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "ome-container",
											Image: "runtime:v1",
										},
									},
								},
							},
						},
						DecoderConfig: &v1beta1.DecoderSpec{
							Leader: &v1beta1.LeaderSpec{
								PodSpec: v1beta1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "ome-container",
											Image: "runtime:v1",
										},
									},
								},
							},
							Worker: &v1beta1.WorkerSpec{
								Size: intPtr(2),
								PodSpec: v1beta1.PodSpec{
									Containers: []v1.Container{
										{
											Name:  "ome-container",
											Image: "runtime:v1",
										},
									},
								},
							},
						},
						RouterConfig: &v1beta1.RouterSpec{
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "router",
										Image: "runtime:v1",
									},
								},
							},
						},
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			buildRequest: func(c client.Client, cs *fake.Clientset, isvc *v1beta1.InferenceService) *WorkloadReconcileRequest {
				// Get BaseModel
				baseModel := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{Name: "base-model-4", Namespace: "default"}, baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Get ServingRuntime
				rt := &v1beta1.ServingRuntime{}
				err = c.Get(context.TODO(), types.NamespacedName{Name: "custom-runtime", Namespace: "default"}, rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create InferenceServiceConfig
				isvcConfig, err := controllerconfig.NewInferenceServicesConfig(cs)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create ComponentBuilderFactory
				factory := components.NewComponentBuilderFactory(c, cs, c.Scheme(), isvcConfig)

				return &WorkloadReconcileRequest{
					InferenceService: isvc,
					BaseModel:        &baseModel.Spec,
					BaseModelMeta:    &baseModel.ObjectMeta,
					Runtime:          &rt.Spec,
					RuntimeName:      rt.Name,
					MergedEngine: &v1beta1.EngineSpec{
						Leader: &v1beta1.LeaderSpec{
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ome-container",
										Image: "runtime:v1",
									},
								},
							},
						},
						Worker: &v1beta1.WorkerSpec{
							Size: intPtr(2),
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ome-container",
										Image: "runtime:v1",
									},
								},
							},
						},
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					MergedDecoder: &v1beta1.DecoderSpec{
						Leader: &v1beta1.LeaderSpec{
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ome-container",
										Image: "runtime:v1",
									},
								},
							},
						},
						Worker: &v1beta1.WorkerSpec{
							Size: intPtr(2),
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ome-container",
										Image: "runtime:v1",
									},
								},
							},
						},
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					MergedRouter: &v1beta1.RouterSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "router",
									Image: "router:v1",
								},
							},
						},
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 1,
						},
					},
					DeploymentModes: &ComponentDeploymentModes{
						Engine:  constants.MultiNode,
						Decoder: constants.MultiNode,
						Router:  constants.RawDeployment,
					},
					ComponentBuilderFactory:     factory,
					UserSpecifiedRuntime:        true,
					EngineSupportedModelFormat:  &rt.Spec.SupportedModelFormats[0],
					DecoderSupportedModelFormat: &rt.Spec.SupportedModelFormats[0],
				}
			},
			validate: func(t *testing.T, c client.Client, isvcName string) {
				// Verify Engine resources
				engineDeployment := &lws.LeaderWorkerSet{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      constants.LWSName(isvcName + "-engine"),
					Namespace: "default",
				}, engineDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Decoder resources
				decoderDeployment := &lws.LeaderWorkerSet{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      constants.LWSName(isvcName + "-decoder"),
					Namespace: "default",
				}, decoderDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Router resources
				routerDeployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-router",
					Namespace: "default",
				}, routerDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify services for all components
				engineService := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-engine",
					Namespace: "default",
				}, engineService)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				decoderService := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-decoder",
					Namespace: "default",
				}, decoderService)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				routerService := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-router",
					Namespace: "default",
				}, routerService)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify Router RBAC
				routerSA := &v1.ServiceAccount{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      isvcName + "-router",
					Namespace: "default",
				}, routerSA)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify all resources have correct labels
				g.Expect(engineDeployment.Labels[constants.InferenceServicePodLabelKey]).To(gomega.Equal(isvcName))
				g.Expect(decoderDeployment.Labels[constants.InferenceServicePodLabelKey]).To(gomega.Equal(isvcName))
				g.Expect(routerDeployment.Labels[constants.InferenceServicePodLabelKey]).To(gomega.Equal(isvcName))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// Setup mocks
			if tt.setupMocks != nil {
				tt.setupMocks(c, clientset)
			}

			// Create InferenceService
			err := c.Create(context.TODO(), tt.isvc)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Build reconcile request
			request := tt.buildRequest(c, clientset, tt.isvc)

			// Create strategy and reconcile
			strategy := NewSingleComponentStrategy(ctrl.Log.WithName("test"))
			result, err := strategy.ReconcileWorkload(context.TODO(), request)

			// Verify reconcile result
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(result.Requeue).To(gomega.BeFalse())
			g.Expect(result.RequeueAfter).To(gomega.BeZero())

			// Run validation
			if tt.validate != nil {
				tt.validate(t, c, tt.isvc.Name)
			}
		})
	}
}

// Helper functions for test setup
func setupCommonMocks(g *gomega.WithT, c client.Client, cs *fake.Clientset, baseModelName string) {
	// Create ConfigMap for InferenceService config
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inferenceservice-config",
			Namespace: "default",
		},
		Data: map[string]string{
			"config": "{}",
		},
	}
	err := c.Create(context.TODO(), cm)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Create ConfigMap in ome namespace for deploy config
	omeCm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inferenceservice-config",
			Namespace: "ome",
		},
		Data: map[string]string{
			"deploy": `{"defaultDeploymentMode": "RawDeployment"}`,
		},
	}
	_, err = cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), omeCm, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Create BaseModel
	baseModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      baseModelName,
			Namespace: "default",
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{
				Name:    "safetensors",
				Version: stringPtr("1.0.0"),
			},
			Storage: &v1beta1.StorageSpec{
				Path: stringPtr("/mnt/models/test"),
			},
		},
	}
	err = c.Create(context.TODO(), baseModel)
	g.Expect(err).NotTo(gomega.HaveOccurred())
}

// Helper functions for creating pointers

func stringPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func intPtr(i int) *int {
	return &i
}
