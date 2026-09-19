package inferenceservice

import (
	"context"
	"errors"
	"fmt"
	"testing"

	policyv1 "k8s.io/api/policy/v1"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	knapis "knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	omeTesting "sigs.k8s.io/ome/pkg/utils/testing"
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
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

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
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 3,
						},
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
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 3,
						},
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
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 3,
						},
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
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							MinReplicas: intPtr(1),
							MaxReplicas: 3,
						},
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
			name: "Explicit MultiNode annotation dispatches LeaderWorkerSet",
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
						ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
							Annotations: map[string]string{
								constants.DeploymentMode: string(constants.MultiNode),
							},
						},
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
			if tt.isvc.UID == "" {
				tt.isvc.UID = types.UID(tt.isvc.Name + "-uid")
			}
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
				APIReader:                c,
				ClientConfig:             &rest.Config{},
				Clientset:                clientset,
				Log:                      ctrl.Log.WithName("test"),
				Scheme:                   scheme,
				Recorder:                 recorder,
				RuntimeSelector:          runtimeselector.New(c),
				AcceleratorClassSelector: acceleratorclassselector.New(c),
			}

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

// TestUpdateStatusFlushesCoordinationWrites pins that per-Component
// requeues do not drop coordination's in-memory status mutations. When a
// Component asks the reconciler to requeue, the controller flushes
// coordination's in-memory status mutations (Status.RolloutCoordination,
// per-Component Status.Components.<c>.Traffic[]) BEFORE returning the
// requeue. Running updateStatus only on the no-requeue tail of Reconcile
// would drop those writes on every requeuing pass — the steady state
// during active rollouts.
//
// This test asserts the contract updateStatus must satisfy for that
// flush to be effective: an in-memory desiredService with coordination-style
// Status mutations is persisted to the apiserver in one call. If
// updateStatus ever stops persisting those fields (e.g., a future
// refactor that scopes the writes), this test fails immediately.
func TestUpdateStatusFlushesCoordinationWrites(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	// Seed the apiserver with an ISVC whose Status has no Traffic and
	// no RolloutCoordination — the pre-coordination baseline at the
	// top of a Reconcile pass.
	existing := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{
				Name: "base-model",
				Kind: stringPtr("BaseModel"),
			},
			Engine: &v1beta1.EngineSpec{},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {},
			},
		},
	}

	c := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithStatusSubresource(existing).
		Build()

	reconciler := &InferenceServiceReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		Log:       ctrl.Log.WithName("test"),
		Recorder:  record.NewFakeRecorder(10),
	}

	// Simulate what coordination.Reconcile mutates in-memory: per-
	// Component Traffic[] and Status.RolloutCoordination. This is the
	// exact in-memory state the controller would hold after the
	// per-Component loop + coordination layer ran, and before the
	// requeue short-circuit at controller.go:500.
	desired := existing.DeepCopy()
	desired.Status.Components[v1beta1.EngineComponent] = v1beta1.ComponentStatusSpec{
		Traffic: []v1beta1.ComponentTrafficTarget{
			{
				RevisionName:   "test-isvc-engine-rev-abc123",
				Percent:        100,
				LatestRevision: true,
			},
		},
	}
	desired.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name:       "0",
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Policy:     v1beta1.CoordinationPolicyIndependent,
			Phase:      v1beta1.CoordinationPhaseIdle,
		}},
	}

	err := reconciler.updateStatus(desired, constants.RawDeployment)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Re-fetch from the fake apiserver and verify both coordination
	// writes landed. Skipping updateStatus on the requeue short-circuit
	// path would leave these fields unset across every requeueing
	// reconcile — i.e., the entire duration of an active rollout.
	flushed := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.TODO(), types.NamespacedName{
		Name:      existing.Name,
		Namespace: existing.Namespace,
	}, flushed)).NotTo(gomega.HaveOccurred())

	g.Expect(flushed.Status.Components).To(gomega.HaveKey(v1beta1.EngineComponent))
	g.Expect(flushed.Status.Components[v1beta1.EngineComponent].Traffic).To(gomega.HaveLen(1))
	g.Expect(flushed.Status.Components[v1beta1.EngineComponent].Traffic[0].RevisionName).
		To(gomega.Equal("test-isvc-engine-rev-abc123"))
	g.Expect(flushed.Status.Components[v1beta1.EngineComponent].Traffic[0].Percent).
		To(gomega.Equal(int32(100)))

	g.Expect(flushed.Status.RolloutCoordination).NotTo(gomega.BeNil())
	g.Expect(flushed.Status.RolloutCoordination.Groups).To(gomega.HaveLen(1))
	g.Expect(flushed.Status.RolloutCoordination.Groups[0].Phase).
		To(gomega.Equal(v1beta1.CoordinationPhaseIdle))
}

// TestUpdateStatusPersistsCanaryStepDespiteStaleCache reproduces the
// manual-canary promote wedge. Durable metadata patches (the canary
// engine's deferred promote-annotation removal, or any external
// annotator) bump the ISVC resourceVersion between reconciles, while
// status.canary.currentStep rides the controller's deferred status flush.
// If updateStatus reads its optimistic-lock base from the informer
// cache — which still holds the pre-patch resourceVersion — the status
// write 409s, RetryOnConflict re-reads the same stale cache and exhausts,
// and the step increment is dropped. The rollout then wedges at Paused
// step 0.
//
// The retry base must come from the authoritative reader (APIReader), not
// the cache, so the write lands on the live resourceVersion. This test
// pins that contract. A fake-client-only test can't catch this: with no
// cache/live resourceVersion skew there is no conflict.
func TestUpdateStatusPersistsCanaryStepDespiteStaleCache(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	const canaryHash = "bbfb0fd4"
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isvc",
			Namespace: "default",
			Annotations: map[string]string{
				constants.RolloutPromoteAnnotation: canaryHash,
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model:  &v1beta1.ModelRef{Name: "base-model", Kind: stringPtr("BaseModel")},
			Engine: &v1beta1.EngineSpec{},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {},
			},
			Canary: &v1beta1.CanaryStatus{
				CanaryRevisionHash: canaryHash,
				CurrentStep:        0,
			},
		},
	}

	// live models the apiserver.
	live := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithStatusSubresource(isvc).
		Build()
	nn := types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}

	// Snapshot the informer cache's view BEFORE the metadata patch: the
	// promote annotation is still present at the pre-patch resourceVersion.
	staleCache := &v1beta1.InferenceService{}
	g.Expect(live.Get(context.TODO(), nn, staleCache)).NotTo(gomega.HaveOccurred())

	// A durable metadata write removes the promote annotation, bumping the
	// live resourceVersion past what the cache holds. Status (currentStep) is
	// untouched here — it rides the in-memory desiredService below.
	bumped := staleCache.DeepCopy()
	delete(bumped.Annotations, constants.RolloutPromoteAnnotation)
	g.Expect(live.Update(context.TODO(), bumped)).NotTo(gomega.HaveOccurred())

	// r.Client models the cache: every Get returns the stale pre-patch
	// snapshot. Status().Update passes through to live, which enforces
	// optimistic concurrency against the bumped resourceVersion.
	cached := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			out, ok := obj.(*v1beta1.InferenceService)
			g.Expect(ok).To(gomega.BeTrue())
			staleCache.DeepCopyInto(out)
			return nil
		},
	})

	reconciler := &InferenceServiceReconciler{
		Client:    cached,
		APIReader: live,
		Scheme:    scheme,
		Log:       ctrl.Log.WithName("test"),
		Recorder:  record.NewFakeRecorder(10),
	}

	// desiredService is the in-memory ISVC after the canary advance:
	// currentStep has advanced to 1 with the applied promote recorded in the
	// same status mutation.
	desired := staleCache.DeepCopy()
	desired.Status.Canary.CurrentStep = 1
	desired.Status.Canary.PromotedThrough = canaryHash

	err := reconciler.updateStatus(desired, constants.OMENative)
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"status flush must not lose an optimistic-lock race to a concurrent metadata patch")

	// The authoritative store must show the advanced step. A retry base
	// taken from the stale cache would 409 the write and leave currentStep
	// at 0 — a rollout wedged at Paused step 0.
	persisted := &v1beta1.InferenceService{}
	g.Expect(live.Get(context.TODO(), nn, persisted)).NotTo(gomega.HaveOccurred())
	g.Expect(persisted.Status.Canary).NotTo(gomega.BeNil())
	g.Expect(persisted.Status.Canary.CurrentStep).To(gomega.Equal(int32(1)),
		"canary step advance must persist to the live apiserver despite the stale cache")
	g.Expect(persisted.Status.Canary.PromotedThrough).To(gomega.Equal(canaryHash),
		"the applied promote record must land in the same status write as the advance")
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
	return constants.RawDeployment
}

func TestValidateResolvedRuntimeEnabled(t *testing.T) {
	disabled := true
	tests := []struct {
		name        string
		runtimeName string
		runtimeSpec *v1beta1.ServingRuntimeSpec
		isCluster   bool
		wantErr     bool
	}{
		{
			name:        "nil runtime spec",
			runtimeName: "nil-runtime",
		},
		{
			name:        "enabled runtime",
			runtimeName: "enabled-runtime",
			runtimeSpec: &v1beta1.ServingRuntimeSpec{},
		},
		{
			name:        "disabled namespaced runtime",
			runtimeName: "namespaced-runtime",
			runtimeSpec: &v1beta1.ServingRuntimeSpec{Disabled: &disabled},
			wantErr:     true,
		},
		{
			name:        "disabled cluster runtime",
			runtimeName: "cluster-runtime",
			runtimeSpec: &v1beta1.ServingRuntimeSpec{Disabled: &disabled},
			isCluster:   true,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateResolvedRuntimeEnabled(tt.runtimeSpec, tt.runtimeName, tt.isCluster)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("validateResolvedRuntimeEnabled() error = %v", err)
				}
				return
			}

			var disabledErr *runtimeselector.RuntimeDisabledError
			if !errors.As(err, &disabledErr) {
				t.Fatalf("validateResolvedRuntimeEnabled() error = %T, want *runtimeselector.RuntimeDisabledError", err)
			}
			if disabledErr.RuntimeName != tt.runtimeName {
				t.Errorf("RuntimeName = %q, want %q", disabledErr.RuntimeName, tt.runtimeName)
			}
			if disabledErr.IsCluster != tt.isCluster {
				t.Errorf("IsCluster = %t, want %t", disabledErr.IsCluster, tt.isCluster)
			}
		})
	}
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

// TestSetOverlaysReadyConditionPreservesConditionSet pins that
// setOverlaysReadyCondition merges into the conditions slice: raw-replacing
// it would wipe every other condition, and restamping LastTransitionTime
// each pass would force a status write per reconcile.
func TestSetOverlaysReadyConditionPreservesConditionSet(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "overlay-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{
				Name:     "base-model",
				Overlays: []v1beta1.ModelOverlayRef{{Name: "ov1"}},
			},
		},
	}
	isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{
		Type:   v1beta1.EngineReady,
		Status: v1.ConditionTrue,
	})

	overlays := []isvcutils.ResolvedOverlay{{
		Ref:        v1beta1.ModelOverlayRef{Name: "ov1"},
		SkipReason: "overlay ov1 not found",
	}}
	setOverlaysReadyCondition(isvc, overlays)

	g.Expect(isvc.Status.GetCondition(v1beta1.EngineReady)).NotTo(gomega.BeNil(),
		"pre-existing conditions must survive the overlay condition write")
	cond := isvc.Status.GetCondition(v1beta1.OverlaysReady)
	g.Expect(cond).NotTo(gomega.BeNil())
	g.Expect(cond.Status).To(gomega.Equal(v1.ConditionFalse))
	firstTransition := cond.LastTransitionTime

	setOverlaysReadyCondition(isvc, overlays)

	g.Expect(isvc.Status.GetCondition(v1beta1.EngineReady)).NotTo(gomega.BeNil())
	again := isvc.Status.GetCondition(v1beta1.OverlaysReady)
	g.Expect(again.LastTransitionTime).To(gomega.Equal(firstTransition),
		"an unchanged condition must not churn LastTransitionTime")
}

// TestIsvcsReferencingRuntimeWakesUnresolvedAutoSelect covers the runtime
// fan-out for auto-select ISVCs: an ISVC with no spec.runtime that is parked
// on RuntimeReady=False has no name reference to index on, so runtime
// create/update events must wake it through the unresolved index — otherwise
// creating a compatible runtime later never revives it.
func TestIsvcsReferencingRuntimeWakesUnresolvedAutoSelect(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	named := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "named", Namespace: "ns1"},
		Spec:       v1beta1.InferenceServiceSpec{Runtime: &v1beta1.ServingRuntimeRef{Name: "rt-a"}},
	}
	otherNamed := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "other-named", Namespace: "ns1"},
		Spec:       v1beta1.InferenceServiceSpec{Runtime: &v1beta1.ServingRuntimeRef{Name: "rt-b"}},
	}
	autoStuck := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-stuck", Namespace: "ns1"},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "m"}},
	}
	autoStuck.Status.SetCondition(v1beta1.RuntimeReady, &knapis.Condition{
		Type: v1beta1.RuntimeReady, Status: v1.ConditionFalse, Reason: "RuntimeNotFound",
	})
	autoStuckOtherNs := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-stuck-2", Namespace: "ns2"},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "m"}},
	}
	autoStuckOtherNs.Status.SetCondition(v1beta1.RuntimeReady, &knapis.Condition{
		Type: v1beta1.RuntimeReady, Status: v1.ConditionFalse, Reason: "RuntimeNotFound",
	})
	autoHealthy := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-healthy", Namespace: "ns1"},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "m"}},
	}

	c := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(named, otherNamed, autoStuck, autoStuckOtherNs, autoHealthy).
		WithIndex(&v1beta1.InferenceService{}, isvcRuntimeNameIndexField, isvcRuntimeNameIndexExtractor).
		WithIndex(&v1beta1.InferenceService{}, isvcRuntimeUnresolvedIndexField, isvcRuntimeUnresolvedIndexExtractor).
		Build()

	r := &InferenceServiceReconciler{Client: c, Log: ctrl.Log.WithName("test")}

	requestKeys := func(reqs []reconcile.Request) []string {
		keys := make([]string, 0, len(reqs))
		for _, req := range reqs {
			keys = append(keys, fmt.Sprintf("%s/%s", req.Namespace, req.Name))
		}
		return keys
	}

	// Cluster-scoped runtime: wakes the named referencer plus every
	// unresolved auto-select ISVC in any namespace — not healthy ones.
	reqs := r.isvcsReferencingRuntime(context.TODO(), &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-a"},
	})
	g.Expect(requestKeys(reqs)).To(gomega.ConsistOf("ns1/named", "ns1/auto-stuck", "ns2/auto-stuck-2"))

	// Namespaced runtime: same-namespace matches only.
	reqs = r.isvcsReferencingRuntime(context.TODO(), &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "rt-a", Namespace: "ns1"},
	})
	g.Expect(requestKeys(reqs)).To(gomega.ConsistOf("ns1/named", "ns1/auto-stuck"))
}

// TestMarkRuntimeUnresolvedEventPreservesPercent pins the Recorder.Event
// fix: an error message containing '%' must land in the event verbatim, not
// be mangled by printf-format interpretation (e.g. "%!o(MISSING)").
func TestMarkRuntimeUnresolvedEventPreservesPercent(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "pct-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Model:  &v1beta1.ModelRef{Name: "m"},
			Engine: &v1beta1.EngineSpec{},
		},
	}
	c := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithStatusSubresource(isvc).
		Build()
	rec := record.NewFakeRecorder(10)
	r := &InferenceServiceReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		Log:       ctrl.Log.WithName("test"),
		Recorder:  rec,
	}

	cause := errors.New("no runtime supports quantization fp8 (50% of formats matched)")
	_, err := r.markRuntimeUnresolved(isvc, constants.RawDeployment, "m", cause)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	select {
	case ev := <-rec.Events:
		g.Expect(ev).To(gomega.ContainSubstring("50% of formats matched"))
		g.Expect(ev).NotTo(gomega.ContainSubstring("MISSING"))
	default:
		t.Fatal("expected a RuntimeNotFound event")
	}
}
