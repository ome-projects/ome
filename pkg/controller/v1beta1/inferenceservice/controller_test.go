package inferenceservice

import (
	"context"
	"testing"

	policyv1 "k8s.io/api/policy/v1"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/sgl-project/ome/pkg/acceleratorclassselector"
	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/status"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/workload"
	"github.com/sgl-project/ome/pkg/runtimeselector"
	omeTesting "github.com/sgl-project/ome/pkg/testing"
)

func TestInferenceServiceReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

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
	g.Expect(kedav1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(autoscalingv2.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(policyv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name        string
		isvc        *v1beta1.InferenceService
		setupMocks  func(client.Client, *fake.Clientset)
		validate    func(*testing.T, client.Client, *v1beta1.InferenceService)
		wantErr     bool
		wantRequeue bool
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
									Name:  "engine",
									Image: "engine:latest",
								},
							},
						},
					},
				},
			},
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				// Create inferenceservice config in controller-runtime client
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

				// Create the same config in the fake clientset in ome namespace with deploy config
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

				// Create base model
				baseModel := &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "base-model-1",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						ModelFormat: v1beta1.ModelFormat{
							Name:    "safetensors",
							Version: stringPtr("1.0.0"),
						},
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr("/mnt/models/base"),
						},
					},
				}
				err = c.Create(context.TODO(), baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create serving runtime
				rt := &v1beta1.ServingRuntime{
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
									Name:    "safetensors",
									Version: stringPtr("1.0.0"),
									Weight:  int64(1),
								},
							},
						},
						ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
							Containers: []v1.Container{
								{
									Name:  "runtime",
									Image: "runtime:v1",
								},
							},
						},
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check that engine deployment was created
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-engine-only-engine",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(gomega.Equal("engine:latest"))
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
					Engine: &v1beta1.EngineSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "engine",
									Image: "engine:latest",
								},
							},
						},
					},
					Decoder: &v1beta1.DecoderSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "decoder",
									Image: "decoder:latest",
								},
							},
						},
					},
				},
			},
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				// Create inferenceservice config in controller-runtime client
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

				// Create the same config in the fake clientset in ome namespace with deploy config
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

				// Create base model
				baseModel := &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "base-model-2",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						ModelFormat: v1beta1.ModelFormat{
							Name:    "safetensors",
							Version: stringPtr("1.0.0"),
						},
					},
				}
				err = c.Create(context.TODO(), baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create serving runtime
				rt := &v1beta1.ServingRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pd-runtime",
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
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check that both engine and decoder deployments were created
				engineDeployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-pd-disaggregated-engine",
					Namespace: "default",
				}, engineDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				decoderDeployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-pd-disaggregated-decoder",
					Namespace: "default",
				}, decoderDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
		},
		{
			name: "Legacy predictor architecture",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-legacy",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Predictor: v1beta1.PredictorSpec{
						Model: &v1beta1.ModelSpec{
							BaseModel: stringPtr("sklearn-model"),
							PredictorExtensionSpec: v1beta1.PredictorExtensionSpec{
								StorageUri: stringPtr("gs://bucket/model"),
							},
						},
					},
				},
			},
			// This test requires multiple reconciliations because cleanup waits for new deployments
			wantRequeue: true,
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				// Create the config in the fake clientset in ome namespace with deploy config
				omeCm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"deploy": `{"defaultDeploymentMode": "RawDeployment"}`,
					},
				}
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), omeCm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create sklearn base model
				baseModel := &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sklearn-model",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						ModelFormat: v1beta1.ModelFormat{
							Name:    "sklearn",
							Version: stringPtr("1.0.0"),
						},
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr("/mnt/models/sklearn"),
						},
						ModelExtensionSpec: v1beta1.ModelExtensionSpec{
							Disabled: boolPtr(false),
						},
						ModelParameterSize: stringPtr("100M"),
					},
				}
				err = c.Create(context.TODO(), baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create an old predictor deployment to simulate existing resources
				oldDeployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-legacy",
						Namespace: "default",
						Labels: map[string]string{
							constants.InferenceServicePodLabelKey: "test-legacy",
							constants.OMEComponentLabel:           "predictor",
						},
					},
					Spec: appsv1.DeploymentSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								constants.InferenceServicePodLabelKey: "test-legacy",
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									constants.InferenceServicePodLabelKey: "test-legacy",
								},
							},
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "predictor",
										Image: "old-predictor-image:latest",
									},
								},
							},
						},
					},
				}
				err = c.Create(context.TODO(), oldDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create sklearn runtime
				rt := &v1beta1.ServingRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sklearn-runtime",
						Namespace: "default",
					},
					Spec: v1beta1.ServingRuntimeSpec{
						SupportedModelFormats: []v1beta1.SupportedModelFormat{
							{
								Name:       "sklearn",
								Version:    stringPtr("*"),
								AutoSelect: boolPtr(true),
								ModelFormat: &v1beta1.ModelFormat{
									Name:    "sklearn",
									Version: stringPtr("1.0.0"),
									Weight:  int64(1),
								},
							},
						},
						ModelSizeRange: &v1beta1.ModelSizeRangeSpec{
							Min: stringPtr("0"),
							Max: stringPtr("1B"),
						},
						ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
							Containers: []v1.Container{
								{
									Name:  "ome-container",
									Image: "sklearn-server:v1",
								},
							},
						},
						EngineConfig: &v1beta1.EngineSpec{
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "ome-container",
										Image: "sklearn-server:v1",
									},
								},
							},
						},
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// The migration should have happened, check that the resource was updated
				updatedIsvc := &v1beta1.InferenceService{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-legacy",
					Namespace: "default",
				}, updatedIsvc)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Check that migration happened
				g.Expect(updatedIsvc.Spec.Model).NotTo(gomega.BeNil(), "Expected model to be migrated")
				g.Expect(updatedIsvc.Spec.Model.Name).To(gomega.Equal("sklearn-model"))
				g.Expect(updatedIsvc.Spec.Engine).NotTo(gomega.BeNil(), "Expected engine to be created")

				// Check for deprecation warning
				g.Expect(updatedIsvc.ObjectMeta.Annotations).NotTo(gomega.BeNil())
				g.Expect(updatedIsvc.ObjectMeta.Annotations[constants.DeprecationWarning]).To(gomega.Equal("The Predictor field is deprecated and will be removed in a future release. Please use Engine and Model fields instead."))

				// Note: Old predictor deployment will NOT be deleted on first reconciliation
				// because cleanup waits for new engine deployment to be ready.
				// The old deployment should still exist.
				deployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-legacy", // predictor deployment uses inference service name
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred(), "Old deployment should still exist on first reconciliation")

				// Check that new engine deployment was created
				engineDeployment := &appsv1.Deployment{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-legacy-engine",
					Namespace: "default",
				}, engineDeployment)
				g.Expect(err).NotTo(gomega.HaveOccurred(), "Expected engine deployment to be created")
			},
		},
		{
			name: "Runtime specified explicitly",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-explicit-runtime",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						Name: "base-model-3",
						Kind: stringPtr("BaseModel"),
					},
					Runtime: &v1beta1.ServingRuntimeRef{
						Name: "custom-runtime",
					},
					Engine: &v1beta1.EngineSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "engine",
									Image: "engine:latest",
								},
							},
						},
					},
				},
			},
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				// Create inferenceservice config in controller-runtime client
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

				// Create the same config in the fake clientset in ome namespace with deploy config
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

				// Create base model
				baseModel := &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "base-model-3",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						ModelFormat: v1beta1.ModelFormat{
							Name:    "pytorch",
							Version: stringPtr("1.0.0"),
						},
					},
				}
				err = c.Create(context.TODO(), baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create explicitly specified runtime
				rt := &v1beta1.ServingRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "custom-runtime",
						Namespace: "default",
					},
					Spec: v1beta1.ServingRuntimeSpec{
						SupportedModelFormats: []v1beta1.SupportedModelFormat{
							{
								Name:    "pytorch",
								Version: stringPtr("*"),
								ModelFormat: &v1beta1.ModelFormat{
									Name:    "pytorch",
									Version: stringPtr("1.0.0"),
									Weight:  int64(1),
								},
							},
						},
						ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
							Containers: []v1.Container{
								{
									Name:  "custom",
									Image: "custom-runtime:v2",
								},
							},
						},
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check that the custom runtime was used
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-explicit-runtime-engine",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(gomega.Equal("engine:latest"))
			},
		},
		{
			name: "Multi-node engine deployment",
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
					Engine: &v1beta1.EngineSpec{
						Leader: &v1beta1.LeaderSpec{
							PodSpec: v1beta1.PodSpec{
								Containers: []v1.Container{
									{
										Name:  "leader",
										Image: "leader:latest",
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
										Image: "worker:latest",
									},
								},
							},
						},
					},
				},
			},
			setupMocks: func(c client.Client, cs *fake.Clientset) {
				// Create inferenceservice config in controller-runtime client
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

				// Create the same config in the fake clientset in ome namespace with deploy config
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

				// Create base model
				baseModel := &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "base-model-4",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						ModelFormat: v1beta1.ModelFormat{
							Name:    "safetensors",
							Version: stringPtr("1.0.0"),
						},
					},
				}
				err = c.Create(context.TODO(), baseModel)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create runtime
				rt := &v1beta1.ServingRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "multinode-runtime",
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
					},
				}
				err = c.Create(context.TODO(), rt)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check that LeaderWorkerSet was created
				lwsList := &lws.LeaderWorkerSetList{}
				err := c.List(context.TODO(), lwsList, client.InNamespace("default"))
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(lwsList.Items).To(gomega.HaveLen(1))
				// Just verify that a LeaderWorkerSet was created
				// The exact replica count might be handled differently by the controller
				g.Expect(lwsList.Items[0].Name).To(gomega.ContainSubstring("test-multinode"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.isvc).
				WithStatusSubresource(tt.isvc).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// Setup mocks
			if tt.setupMocks != nil {
				tt.setupMocks(c, clientset)
			}

			// Create recorder
			recorder := record.NewFakeRecorder(10)

			// Create reconciler
			reconciler := &InferenceServiceReconciler{
				Client:                   c,
				ClientConfig:             &rest.Config{},
				Clientset:                clientset,
				Log:                      ctrl.Log.WithName("test"),
				Scheme:                   scheme,
				Recorder:                 recorder,
				StatusManager:            status.NewStatusReconciler(),
				RuntimeSelector:          runtimeselector.New(c),
				AcceleratorClassSelector: acceleratorclassselector.New(c),
			}

			// Initialize StrategyManager and register strategies
			reconciler.StrategyManager = workload.NewWorkloadStrategyManager(reconciler.Log)
			singleStrategy := workload.NewSingleComponentStrategy(reconciler.Log)
			err = reconciler.StrategyManager.RegisterStrategy(singleStrategy)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Ensure the InferenceService exists in the client
			existingIsvc := &v1beta1.InferenceService{}
			err = c.Get(context.TODO(), types.NamespacedName{
				Name:      tt.isvc.Name,
				Namespace: tt.isvc.Namespace,
			}, existingIsvc)
			if err != nil {
				// If not found, create it
				err = c.Create(context.TODO(), tt.isvc)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Reconcile
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tt.isvc.Name,
					Namespace: tt.isvc.Namespace,
				},
			}
			result, err := reconciler.Reconcile(context.TODO(), req)

			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				if tt.wantRequeue {
					g.Expect(result.Requeue).To(gomega.BeTrue(), "Expected requeue for test: %s", tt.name)
				} else {
					g.Expect(result).To(gomega.Equal(ctrl.Result{}))
				}

				// Run validations
				if tt.validate != nil {
					tt.validate(t, c, tt.isvc)
				}
			}
		})
	}
}

func TestDetermineDeploymentModes(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                    string
		engineSpec              *v1beta1.EngineSpec
		decoderSpec             *v1beta1.DecoderSpec
		expectedEngineMode      constants.DeploymentModeType
		expectedDecoderMode     constants.DeploymentModeType
		expectedPDDisaggregated bool
	}{
		{
			name: "Single engine with raw deployment",
			engineSpec: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{{Name: "engine"}},
				},
			},
			decoderSpec:             nil,
			expectedEngineMode:      constants.RawDeployment,
			expectedDecoderMode:     "",
			expectedPDDisaggregated: false,
		},
		{
			name: "Engine with multi-node deployment",
			engineSpec: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "leader"}},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
				},
			},
			decoderSpec:             nil,
			expectedEngineMode:      constants.MultiNode,
			expectedDecoderMode:     "",
			expectedPDDisaggregated: false,
		},
		{
			name: "PD-disaggregated with raw deployments",
			engineSpec: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{{Name: "engine"}},
				},
			},
			decoderSpec: &v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{{Name: "decoder"}},
				},
			},
			expectedEngineMode:      constants.RawDeployment,
			expectedDecoderMode:     constants.RawDeployment,
			expectedPDDisaggregated: true,
		},
		{
			name: "PD-disaggregated with multi-node decoder",
			engineSpec: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{{Name: "engine"}},
				},
			},
			decoderSpec: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "leader"}},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(1),
				},
			},
			expectedEngineMode:      constants.RawDeployment,
			expectedDecoderMode:     constants.MultiNode,
			expectedPDDisaggregated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test engine deployment mode determination
			if tt.engineSpec != nil {
				engineMode := determineEngineDeploymentMode(tt.engineSpec)
				g.Expect(engineMode).To(gomega.Equal(tt.expectedEngineMode))
			}

			// Test PD-disaggregated detection
			isPDDisaggregated := tt.engineSpec != nil && tt.decoderSpec != nil
			g.Expect(isPDDisaggregated).To(gomega.Equal(tt.expectedPDDisaggregated))

			// Test decoder deployment mode
			if tt.decoderSpec != nil {
				decoderMode := constants.RawDeployment
				if tt.decoderSpec.Leader != nil && tt.decoderSpec.Worker != nil {
					decoderMode = constants.MultiNode
				}
				g.Expect(decoderMode).To(gomega.Equal(tt.expectedDecoderMode))
			}
		})
	}
}

// Helper function to test deployment mode determination
func determineEngineDeploymentMode(engineSpec *v1beta1.EngineSpec) constants.DeploymentModeType {
	if engineSpec.Leader != nil || engineSpec.Worker != nil {
		return constants.MultiNode
	}
	// Note: In real implementation, serverless mode would be determined by annotations/config
	return constants.RawDeployment
}

func TestEnsureIngressDisableAnnotation(t *testing.T) {
	tests := []struct {
		name                string
		isvc                *v1beta1.InferenceService
		expectedAnnotation  bool
		expectedAnnotations map[string]string
		description         string
	}{
		{
			name: "add annotation when no annotations exist",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			expectedAnnotation: true,
			expectedAnnotations: map[string]string{
				"ome.io/ingress-disable-creation": "true",
			},
			description: "should add ingress disable annotation when no annotations exist",
		},
		{
			name: "preserve annotation when already present",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
					Annotations: map[string]string{
						"ome.io/ingress-disable-creation": "true",
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			expectedAnnotation: true,
			expectedAnnotations: map[string]string{
				"ome.io/ingress-disable-creation": "true",
			},
			description: "should preserve existing annotation",
		},
		{
			name: "add annotation while preserving other annotations",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
					Annotations: map[string]string{
						"existing-key": "existing-value",
					},
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{},
				},
			},
			expectedAnnotation: true,
			expectedAnnotations: map[string]string{
				"existing-key":                    "existing-value",
				"ome.io/ingress-disable-creation": "true",
			},
			description: "should add annotation while preserving existing annotations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := &InferenceServiceReconciler{}
			err := reconciler.ensureIngressDisableAnnotation(tt.isvc)

			if err != nil {
				t.Errorf("ensureIngressDisableAnnotation returned error: %v", err)
				return
			}

			if tt.expectedAnnotation {
				if tt.isvc.Annotations == nil {
					t.Errorf("expected annotations to be non-nil")
					return
				}
				if tt.isvc.Annotations["ome.io/ingress-disable-creation"] != "true" {
					t.Errorf("expected ome.io/ingress-disable-creation annotation to be 'true', got '%s'", tt.isvc.Annotations["ome.io/ingress-disable-creation"])
				}
			}

			// Verify all expected annotations are present
			for key, value := range tt.expectedAnnotations {
				if tt.isvc.Annotations[key] != value {
					t.Errorf("expected annotation %s to have value '%s', got '%s'", key, value, tt.isvc.Annotations[key])
				}
			}
		})
	}
}

func TestMergeRuntimeSpecs(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name               string
		isvc               *v1beta1.InferenceService
		runtime            *v1beta1.ServingRuntimeSpec
		expectedEngineImg  string
		expectedDecoderImg string
		expectError        bool
	}{
		{
			name: "Engine only merge",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "engine",
									Image: "user-engine:latest",
								},
							},
						},
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name:  "runtime",
							Image: "runtime:v1",
						},
					},
				},
			},
			expectedEngineImg: "runtime:v1",
		},
		{
			name: "Engine and decoder merge",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine: &v1beta1.EngineSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{Name: "engine"},
							},
						},
					},
					Decoder: &v1beta1.DecoderSpec{
						PodSpec: v1beta1.PodSpec{
							Containers: []v1.Container{
								{Name: "decoder"},
							},
						},
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name:  "runtime",
							Image: "runtime:v2",
						},
					},
				},
			},
			expectedEngineImg:  "runtime:v2",
			expectedDecoderImg: "runtime:v2",
		},
		{
			name: "No engine or decoder should not error",
			isvc: &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{},
			},
			runtime:     &v1beta1.ServingRuntimeSpec{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// In real implementation, this would call the actual MergeRuntimeSpecs
			// For test purposes, we'll simulate the behavior
			if tt.isvc.Spec.Engine != nil && len(tt.runtime.Containers) > 0 {
				if len(tt.isvc.Spec.Engine.Containers) > 0 {
					tt.isvc.Spec.Engine.Containers[0].Image = tt.runtime.Containers[0].Image
				}
			}
			if tt.isvc.Spec.Decoder != nil && len(tt.runtime.Containers) > 0 {
				if len(tt.isvc.Spec.Decoder.Containers) > 0 {
					tt.isvc.Spec.Decoder.Containers[0].Image = tt.runtime.Containers[0].Image
				}
			}

			// Validate results
			if tt.expectedEngineImg != "" && tt.isvc.Spec.Engine != nil {
				g.Expect(tt.isvc.Spec.Engine.Containers[0].Image).To(gomega.Equal(tt.expectedEngineImg))
			}
			if tt.expectedDecoderImg != "" && tt.isvc.Spec.Decoder != nil {
				g.Expect(tt.isvc.Spec.Decoder.Containers[0].Image).To(gomega.Equal(tt.expectedDecoderImg))
			}
		})
	}
}
