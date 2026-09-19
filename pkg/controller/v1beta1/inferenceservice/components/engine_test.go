package components

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// Helper functions for creating pointers
func intPtr(i int) *int {
	return &i
}

func stringPtr(s string) *string {
	return &s
}

func TestEngineReconcileDeployment_ProjectsMergedTopologyKey(t *testing.T) {
	g := gomega.NewWithT(t)
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		v1beta1.AddToScheme,
		v1.AddToScheme,
		appsv1.AddToScheme,
		autoscalingv2.AddToScheme,
		policyv1.AddToScheme,
		kedav1.AddToScheme,
	} {
		g.Expect(add(scheme)).To(gomega.Succeed())
	}
	c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	workerSize := 1
	topologyKey := "topology.example.com/domain"
	engineSpec := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(1)},
		Leader:                 &v1beta1.LeaderSpec{},
		Worker:                 &v1beta1.WorkerSpec{Size: &workerSize},
		TopologyKey:            &topologyKey,
	}
	engine := NewEngine(
		&ComponentDeps{Client: c, Clientset: fake.NewClientset(), APIReader: c, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
		ComponentInputs{DeploymentMode: constants.OMENative},
		engineSpec,
	).(*Engine)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "topology", Namespace: "default", UID: types.UID("topology-uid")},
	}
	g.Expect(c.Create(context.Background(), &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Name: "topology-engine", Namespace: "default", UID: types.UID("topology-engine-uid"),
	}})).To(gomega.Succeed())
	leaderPodSpec := &v1.PodSpec{Containers: []v1.Container{{Name: "leader", Image: "test:v1"}}}
	workerPodSpec := &v1.PodSpec{Containers: []v1.Container{{Name: "worker", Image: "test:v1"}}}
	componentMeta := metav1.ObjectMeta{
		Name:      "topology-engine",
		Namespace: "default",
	}
	request := mustComponentPDBRequest(t, &engine.BaseComponentFields, isvc, constants.OMENative, v1beta1.EngineComponent, componentMeta, &engineSpec.ComponentExtensionSpec)

	_, err := engine.reconcileDeployment(context.Background(), isvc, componentMeta, leaderPodSpec, 1, workerPodSpec, request)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	ir := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "topology-engine"}, ir)).To(gomega.Succeed())
	g.Expect(ir.Spec.TopologyKey).NotTo(gomega.BeNil())
	g.Expect(*ir.Spec.TopologyKey).To(gomega.Equal(topologyKey))
}

func TestEngineReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

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
		name           string
		deploymentMode constants.DeploymentModeType
		baseModel      *v1beta1.BaseModelSpec
		baseModelMeta  *metav1.ObjectMeta
		engineSpec     *v1beta1.EngineSpec
		runtime        *v1beta1.ServingRuntimeSpec
		runtimeName    string
		isvc           *v1beta1.InferenceService
		setupMocks     func(client.Client, kubernetes.Interface)
		validate       func(*testing.T, client.Client, *v1beta1.InferenceService)
		wantErr        bool
	}{
		{
			name:           "Raw deployment with basic engine spec",
			deploymentMode: constants.RawDeployment,
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/model1"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "base-model-1",
				Namespace: "default",
				Annotations: map[string]string{
					constants.ModelCategoryAnnotation: "LARGE",
				},
			},
			engineSpec: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 3,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "engine:latest",
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
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "runtime:latest",
							Env: []v1.EnvVar{
								{Name: "RUNTIME_ENV", Value: "test"},
							},
						},
					},
				},
			},
			runtimeName: "test-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			setupMocks: func(c client.Client, cs kubernetes.Interface) {
				// Create inferenceservice config in both clients
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"config": "{}",
					},
				}
				// Create in controller-runtime client
				err := c.Create(context.TODO(), cm)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Also create in clientset (if different)
				_, err = cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil && !strings.Contains(err.Error(), "already exists") {
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check deployment was created
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-isvc-engine",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(gomega.Equal("engine:latest"))
				// For a raw deployment test without a runner specified, we shouldn't expect MODEL_PATH
				// since environment variables are only applied to the runner container
				g.Expect(deployment.Spec.Template.Spec.Containers[0].Env).To(gomega.BeEmpty())

				// Check node selector was added for the base model
				g.Expect(deployment.Spec.Template.Spec.NodeSelector).NotTo(gomega.BeNil())
				value, found := deployment.Spec.Template.Spec.NodeSelector["models.ome.io/default.basemodel.base-model-1"]
				g.Expect(found).To(gomega.BeTrue(), "Model node selector not found")
				g.Expect(value).To(gomega.Equal("Ready"))

				// Check service was created
				service := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-isvc-engine",
					Namespace: "default",
				}, service)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
		},
		{
			name:           "Multi-node deployment with leader and worker specs",
			deploymentMode: constants.MultiNode,
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/model2"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "base-model-2",
				Namespace: "default",
			},
			engineSpec: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(2),
				},
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "leader-container",
								Image: "leader:latest",
							},
						},
					},
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Name:  "leader-container",
							Image: "runtime-leader:latest",
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker-container",
								Image: "worker:latest",
							},
						},
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name:  "runtime-container",
							Image: "runtime:latest",
						},
					},
				},
			},
			runtimeName: "multi-node-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-mn-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			setupMocks: func(c client.Client, cs kubernetes.Interface) {
				// Create inferenceservice config in both clients
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"config": "{}",
					},
				}
				// Create in controller-runtime client
				err := c.Create(context.TODO(), cm)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Also create in clientset (if different)
				_, err = cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil && !strings.Contains(err.Error(), "already exists") {
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check LeaderWorkerSet was created
				lwsList := &lws.LeaderWorkerSetList{}
				err := c.List(context.TODO(), lwsList, client.InNamespace("default"))
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(lwsList.Items).To(gomega.HaveLen(1))
				g.Expect(lwsList.Items[0].Spec.Replicas).To(gomega.Equal(int32Ptr(2)))

				// Check node selector was added for both leader and worker pods
				leaderNodeSelector := lwsList.Items[0].Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.NodeSelector
				workerNodeSelector := lwsList.Items[0].Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.NodeSelector
				g.Expect(leaderNodeSelector).NotTo(gomega.BeNil())
				g.Expect(workerNodeSelector).NotTo(gomega.BeNil())

				// Verify leader pod has model node selector
				leaderValue, leaderFound := leaderNodeSelector["models.ome.io/default.basemodel.base-model-2"]
				g.Expect(leaderFound).To(gomega.BeTrue(), "Leader model node selector not found")
				g.Expect(leaderValue).To(gomega.Equal("Ready"))

				// Verify worker pod has model node selector
				workerValue, workerFound := workerNodeSelector["models.ome.io/default.basemodel.base-model-2"]
				g.Expect(workerFound).To(gomega.BeTrue(), "Worker model node selector not found")
				g.Expect(workerValue).To(gomega.Equal("Ready"))
			},
		},
		{
			name:           "Fine-tuned serving with single weight",
			deploymentMode: constants.RawDeployment,
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/base"),
				},
				ModelExtensionSpec: v1beta1.ModelExtensionSpec{
					Vendor: stringPtr("meta"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name: "llama-base",
			},
			engineSpec: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 1,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ft-container",
							Image: "ft:latest",
						},
					},
				},
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "ft-container", // Using same name so it will match and merge
						Image: "ft:latest",
					},
				},
			},
			runtime:     &v1beta1.ServingRuntimeSpec{},
			runtimeName: "ft-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-ft-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{
						FineTunedWeights: []string{"ft-weight-1"},
					},
				},
			},
			setupMocks: func(c client.Client, cs kubernetes.Interface) {
				// Create inferenceservice config in both clients
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"config": "{}",
					},
				}
				// Create in controller-runtime client
				err := c.Create(context.TODO(), cm)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Also create in clientset (if different)
				_, err = cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil && !strings.Contains(err.Error(), "already exists") {
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-ft-isvc-engine",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Check annotations
				annotations := deployment.Spec.Template.Annotations
				g.Expect(annotations[constants.FineTunedAdapterInjectionKey]).To(gomega.Equal("ft-weight-1"))
				g.Expect(annotations[constants.FineTunedWeightFTStrategyKey]).To(gomega.Equal("lora"))

				// Check volume mounts
				container := deployment.Spec.Template.Spec.Containers[0]
				var hasModelMount bool
				for _, vm := range container.VolumeMounts {
					if vm.Name == constants.ModelEmptyDirVolumeName {
						hasModelMount = true
						break
					}
				}
				g.Expect(hasModelMount).To(gomega.BeTrue())
			},
		},
		{
			name:           "ClusterBaseModel with node selector",
			deploymentMode: constants.RawDeployment,
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/cluster-model"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name: "cluster-base-model",
				// No namespace for ClusterBaseModel
			},
			engineSpec: &v1beta1.EngineSpec{
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
			runtime:     &v1beta1.ServingRuntimeSpec{},
			runtimeName: "test-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			setupMocks: func(c client.Client, cs kubernetes.Interface) {
				// Create inferenceservice config
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"config": "{}",
					},
				}
				err := c.Create(context.TODO(), cm)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				_, err = cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				if err != nil && !strings.Contains(err.Error(), "already exists") {
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check deployment was created
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-cluster-isvc-engine",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Check node selector for ClusterBaseModel (no namespace in label)
				g.Expect(deployment.Spec.Template.Spec.NodeSelector).NotTo(gomega.BeNil())
				value, found := deployment.Spec.Template.Spec.NodeSelector["models.ome.io/clusterbasemodel.cluster-base-model"]
				g.Expect(found).To(gomega.BeTrue(), "Model node selector not found")
				g.Expect(value).To(gomega.Equal("Ready"))
			},
		},
		{
			name:           "Engine with nil spec should error",
			deploymentMode: constants.RawDeployment,
			engineSpec:     nil,
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-nil-isvc",
					Namespace: "default",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.isvc != nil && tt.isvc.UID == "" {
				tt.isvc.UID = types.UID(tt.isvc.Name + "-uid")
			}
			// Create objects to add to fake client
			objects := []client.Object{tt.isvc}

			// For fine-tuned serving test, add the FineTunedWeight
			if tt.name == "Fine-tuned serving with single weight" {
				ftWeight := &v1beta1.FineTunedWeight{
					ObjectMeta: metav1.ObjectMeta{
						Name: "ft-weight-1",
					},
					Spec: v1beta1.FineTunedWeightSpec{
						HyperParameters: runtime.RawExtension{
							Raw: []byte(`{"strategy": "lora"}`),
						},
						Configuration: runtime.RawExtension{
							Raw: []byte(`{}`), // Empty config to avoid JSON parsing error
						},
					},
				}
				objects = append(objects, ftWeight)
			}

			// Create fake client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// Setup mocks if needed
			if tt.setupMocks != nil {
				tt.setupMocks(c, clientset)
			}

			// Create engine using the constructor
			engine := NewEngine(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode: tt.deploymentMode,
					BaseModel:      tt.baseModel,
					BaseModelMeta:  tt.baseModelMeta,
					RuntimeName:    tt.runtimeName,
				},
				tt.engineSpec,
			).(*Engine)

			// Set fine-tuned fields if needed
			if tt.name == "Fine-tuned serving with single weight" {
				engine.FineTunedServing = true
			}

			// Reconcile
			result, err := engine.Reconcile(context.TODO(), tt.isvc)

			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))

				// Run validations
				if tt.validate != nil {
					tt.validate(t, c, tt.isvc)
				}
			}
		})
	}
}

func TestEngineReconcileObjectMeta(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                string
		isvc                *v1beta1.InferenceService
		engineSpec          *v1beta1.EngineSpec
		baseModel           *v1beta1.BaseModelSpec
		baseModelMeta       *metav1.ObjectMeta
		runtimeName         string
		fineTunedServing    bool
		fineTunedWeights    []*v1beta1.FineTunedWeight
		expectedAnnotations map[string]string
		expectedLabels      map[string]string
		expectedName        string
	}{
		{
			name: "Basic object metadata",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					Annotations: map[string]string{
						"custom-annotation": "value",
					},
					Labels: map[string]string{
						"custom-label": "value",
					},
				},
			},
			engineSpec: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{
						"engine-annotation": "engine-value",
					},
					Labels: map[string]string{
						"engine-label": "engine-value",
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name:    "safetensors",
					Version: stringPtr("1.0"),
				},
				ModelExtensionSpec: v1beta1.ModelExtensionSpec{
					Vendor: stringPtr("meta"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name: "base-model",
				Annotations: map[string]string{
					constants.ModelCategoryAnnotation: "LARGE",
				},
			},
			runtimeName: "test-runtime",
			expectedAnnotations: map[string]string{
				"custom-annotation":                    "value",
				"engine-annotation":                    "engine-value",
				constants.BaseModelName:                "base-model",
				constants.BaseModelFormat:              "safetensors",
				constants.BaseModelFormatVersion:       "1.0",
				constants.BaseModelVendorAnnotationKey: "meta",
				constants.ServingRuntimeKeyName:        "test-runtime",
			},
			expectedLabels: map[string]string{
				"custom-label":                                  "value",
				"engine-label":                                  "engine-value",
				constants.InferenceServicePodLabelKey:           "test-isvc",
				constants.OMEComponentLabel:                     "engine",
				constants.ServingRuntimeLabelKey:                "test-runtime",
				constants.InferenceServiceBaseModelNameLabelKey: "base-model",
				constants.InferenceServiceBaseModelSizeLabelKey: "LARGE",
				constants.BaseModelTypeLabelKey:                 "Serving",
				constants.BaseModelVendorLabelKey:               "meta",
				constants.FTServingLabelKey:                     "false",
			},
			expectedName: "test-isvc-engine",
		},
		{
			name: "Fine-tuned serving metadata",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ft-isvc",
					Namespace: "default",
				},
			},
			engineSpec: &v1beta1.EngineSpec{},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
				ModelExtensionSpec: v1beta1.ModelExtensionSpec{
					Vendor: stringPtr("meta"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name: "llama-base",
			},
			fineTunedServing: true,
			fineTunedWeights: []*v1beta1.FineTunedWeight{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "ft-weight",
					},
					Spec: v1beta1.FineTunedWeightSpec{
						HyperParameters: runtime.RawExtension{
							Raw: []byte(`{"strategy": "lora"}`),
						},
					},
				},
			},
			expectedAnnotations: map[string]string{
				constants.FineTunedAdapterInjectionKey: "ft-weight",
				constants.FineTunedWeightFTStrategyKey: "lora",
				constants.BaseModelName:                "llama-base",
				constants.BaseModelFormat:              "safetensors",
				constants.BaseModelVendorAnnotationKey: "meta",
			},
			expectedLabels: map[string]string{
				constants.InferenceServicePodLabelKey:           "ft-isvc",
				constants.OMEComponentLabel:                     "engine",
				constants.FTServingLabelKey:                     "true",
				constants.FineTunedWeightFTStrategyLabelKey:     "lora",
				constants.FTServingWithMergedWeightsLabelKey:    "false",
				constants.InferenceServiceBaseModelNameLabelKey: "llama-base",
				constants.InferenceServiceBaseModelSizeLabelKey: "SMALL",
				constants.BaseModelTypeLabelKey:                 "Serving",
				constants.BaseModelVendorLabelKey:               "meta",
			},
			expectedName: "ft-isvc-engine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scheme
			scheme := runtime.NewScheme()
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			// Create engine using the constructor
			engine := NewEngine(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode: constants.RawDeployment,
					BaseModel:      tt.baseModel,
					BaseModelMeta:  tt.baseModelMeta,
					RuntimeName:    tt.runtimeName,
				},
				tt.engineSpec,
			).(*Engine)

			// Set fine-tuned fields if needed
			if tt.fineTunedServing {
				engine.FineTunedServing = tt.fineTunedServing
			}
			if tt.fineTunedWeights != nil {
				engine.FineTunedWeights = tt.fineTunedWeights
			}

			// Test reconcileObjectMeta
			objectMeta, err := engine.reconcileObjectMeta(context.TODO(), tt.isvc)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Validate name
			g.Expect(objectMeta.Name).To(gomega.Equal(tt.expectedName))
			g.Expect(objectMeta.Namespace).To(gomega.Equal("default"))

			// Validate annotations
			for k, v := range tt.expectedAnnotations {
				g.Expect(objectMeta.Annotations).To(gomega.HaveKeyWithValue(k, v))
			}

			// Validate labels
			for k, v := range tt.expectedLabels {
				g.Expect(objectMeta.Labels).To(gomega.HaveKeyWithValue(k, v))
			}
		})
	}
}

func TestEngineWorkerPodSpec(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name               string
		engineSpec         *v1beta1.EngineSpec
		expectedError      bool
		expectedContainers int
		validatePodSpec    func(*v1.PodSpec)
	}{
		{
			name: "Worker with leader runner",
			engineSpec: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Name:  "leader-runner",
							Image: "leader-runtime:latest",
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker-container",
								Image: "worker:latest",
							},
						},
					},
				},
			},
			expectedContainers: 1,
			validatePodSpec: func(ps *v1.PodSpec) {
				// Should have worker container (leader runner is not merged into worker pod spec)
				found := false
				for _, c := range ps.Containers {
					if c.Name == "worker-container" {
						found = true
						g.Expect(c.Image).To(gomega.Equal("worker:latest"))
					}
				}
				g.Expect(found).To(gomega.BeTrue())
			},
		},
		{
			name: "Worker without leader runner",
			engineSpec: &v1beta1.EngineSpec{
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(1),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker-container",
								Image: "worker:latest",
							},
						},
					},
				},
			},
			expectedContainers: 1,
			validatePodSpec: func(ps *v1.PodSpec) {
				g.Expect(ps.Containers[0].Name).To(gomega.Equal("worker-container"))
				g.Expect(ps.Containers[0].Image).To(gomega.Equal("worker:latest"))
			},
		},
		{
			name:       "No worker spec returns nil",
			engineSpec: &v1beta1.EngineSpec{},
			validatePodSpec: func(ps *v1.PodSpec) {
				g.Expect(ps).To(gomega.BeNil())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			}

			objectMeta := &metav1.ObjectMeta{}

			// Create scheme
			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			// Create engine using the constructor
			engine := NewEngine(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{DeploymentMode: constants.RawDeployment},
				tt.engineSpec,
			).(*Engine)

			podSpec, err := engine.reconcileWorkerPodSpec(isvc, objectMeta)

			if tt.expectedError {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				if tt.validatePodSpec != nil {
					tt.validatePodSpec(podSpec)
				}
			}
		})
	}
}

// TestEngineUsesLeaderTemplate pins the structural predicate that drives
// pod-template selection in reconcilePodSpec. Switching on
// isvcutils.DetermineEngineDeploymentMode(spec) instead would return
// MultiNode for ANY spec with Leader/Worker set, regardless of the
// authoritative DeploymentMode wired in at construction time; for an
// OMENative-mode engine with Leader != nil the two disagree (helper says
// MultiNode; dispatch says OMENative), and only the MultiNode branch's
// incidental use of Leader.PodSpec would hide it. engineUsesLeaderTemplate
// decouples template selection from dispatch-mode classification.
func TestEngineUsesLeaderTemplate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name     string
		spec     *v1beta1.EngineSpec
		expected bool
	}{
		{
			name:     "nil spec returns false",
			spec:     nil,
			expected: false,
		},
		{
			name:     "empty spec — no leader — false",
			spec:     &v1beta1.EngineSpec{},
			expected: false,
		},
		{
			name: "leader set — true (regardless of mode)",
			spec: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "leader"}},
					},
				},
			},
			expected: true,
		},
		{
			name: "leader + worker set — true",
			spec: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "leader"}},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
				},
			},
			expected: true,
		},
		{
			name: "worker only — false (template still from top-level)",
			spec: &v1beta1.EngineSpec{
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g.Expect(engineUsesLeaderTemplate(tt.spec)).To(gomega.Equal(tt.expected))
		})
	}
}

// TestEngineReconcilePodSpec_OMENativeWithLeader pins that the
// OMENative-mode engine with Leader != nil picks the Leader pod template
// (not the top-level engine spec template). Template selection must not
// ride on the MultiNode branch's incidental use of Leader.PodSpec, so a
// refactor of that branch to do something MultiNode-specific does NOT
// silently break the OMENative + Leader path.
func TestEngineReconcilePodSpec_OMENativeWithLeader(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	engineSpec := &v1beta1.EngineSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []v1.Container{
				{Name: "fallback-container", Image: "fallback:latest"},
			},
		},
		Leader: &v1beta1.LeaderSpec{
			PodSpec: v1beta1.PodSpec{
				Containers: []v1.Container{
					{Name: "leader-container", Image: "leader:latest"},
				},
			},
		},
		Worker: &v1beta1.WorkerSpec{
			Size: intPtr(2),
			PodSpec: v1beta1.PodSpec{
				Containers: []v1.Container{
					{Name: "worker-container", Image: "worker:latest"},
				},
			},
		},
	}

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
	}
	objectMeta := &metav1.ObjectMeta{Name: "test-isvc-engine", Namespace: "default"}

	clientset := fake.NewClientset()
	c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

	// Construct with DeploymentMode=OMENative — distinct from what
	// DetermineEngineDeploymentMode would return for this spec (MultiNode).
	engine := NewEngine(
		&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
		ComponentInputs{DeploymentMode: constants.OMENative},
		engineSpec,
	).(*Engine)

	podSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(podSpec).NotTo(gomega.BeNil())
	g.Expect(podSpec.Containers).To(gomega.HaveLen(1))
	g.Expect(podSpec.Containers[0].Name).To(gomega.Equal("leader-container"),
		"OMENative engine with Leader must source pod template from Leader.PodSpec, not engineSpec.PodSpec")
	g.Expect(podSpec.Containers[0].Image).To(gomega.Equal("leader:latest"))
}

// TestEngineReconcilePodSpec_RuntimeSchedulerName renders engine pod
// templates from specs merged out of a runtime whose schedulerName is
// set only at the top level, and pins that every rendered corev1
// PodSpec carries it. The gang machinery and the scheduler both read
// the rendered specs — a name lost between the runtime and the
// template silently lands the pods on the default scheduler.
func TestEngineReconcilePodSpec_RuntimeSchedulerName(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	newEngineFromMerged := func(mergedEngine *v1beta1.EngineSpec, mode constants.DeploymentModeType) *Engine {
		return NewEngine(
			&ComponentDeps{
				Client:    ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build(),
				Clientset: fake.NewClientset(),
				Scheme:    scheme,
				Config:    &controllerconfig.InferenceServicesConfig{},
			},
			ComponentInputs{DeploymentMode: mode},
			mergedEngine,
		).(*Engine)
	}

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	objectMeta := &metav1.ObjectMeta{Name: "test-isvc-engine", Namespace: "default"}

	multiPodRuntime := func(leaderSchedulerName string) *v1beta1.ServingRuntimeSpec {
		return &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "custom-scheduler"},
			EngineConfig: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{SchedulerName: leaderSchedulerName},
					Runner:  &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container", Image: "leader:latest"}},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "ome-container", Image: "worker:latest"}},
					},
				},
			},
		}
	}

	t.Run("rendered leader and worker templates carry the top-level name", func(t *testing.T) {
		mergedEngine, _, _, err := isvcutils.MergeRuntimeSpecs(isvc.DeepCopy(), multiPodRuntime(""), logr.Discard())
		g.Expect(err).NotTo(gomega.HaveOccurred())

		engine := newEngineFromMerged(mergedEngine, constants.OMENative)

		podSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(podSpec.SchedulerName).To(gomega.Equal("custom-scheduler"))

		workerPodSpec, err := engine.reconcileWorkerPodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(workerPodSpec.SchedulerName).To(gomega.Equal("custom-scheduler"))
	})

	t.Run("leader-level name overrides the top-level name", func(t *testing.T) {
		mergedEngine, _, _, err := isvcutils.MergeRuntimeSpecs(isvc.DeepCopy(), multiPodRuntime("leader-scheduler"), logr.Discard())
		g.Expect(err).NotTo(gomega.HaveOccurred())

		engine := newEngineFromMerged(mergedEngine, constants.OMENative)

		podSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(podSpec.SchedulerName).To(gomega.Equal("leader-scheduler"))

		workerPodSpec, err := engine.reconcileWorkerPodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(workerPodSpec.SchedulerName).To(gomega.Equal("custom-scheduler"),
			"a worker level left unset must still fall back to the top-level name")
	})

	t.Run("rendered single-pod template carries the top-level name", func(t *testing.T) {
		rt := &v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{SchedulerName: "custom-scheduler"},
			EngineConfig: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{Name: "ome-container", Image: "engine:latest"}},
			},
		}
		mergedEngine, _, _, err := isvcutils.MergeRuntimeSpecs(isvc.DeepCopy(), rt, logr.Discard())
		g.Expect(err).NotTo(gomega.HaveOccurred())

		engine := newEngineFromMerged(mergedEngine, constants.RawDeployment)

		podSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(podSpec.SchedulerName).To(gomega.Equal("custom-scheduler"))
	})
}

// TestEngineDetermineEngineName_CtxCancellation pins that context
// cancellation propagates through the name-discovery Get call. The earlier
// implementation hardcoded context.TODO() inside a controller reconcile
// flow, which silently ignored the reconcile manager's cancellation
// signal.
//
// With a canceled ctx the fake client's Get returns immediately with the
// ctx error — the determineEngineName helper does NOT propagate that error
// (it falls back to the default name), but the test pins that the function
// returns promptly (no hang) AND uses the supplied ctx (the ctx parameter
// is wired through to the client Get call; a stray context.TODO would
// shadow it and the cancellation signal would be lost).
func TestEngineDetermineEngineName_CtxCancellation(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	clientset := fake.NewClientset()

	engine := NewEngine(
		&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
		ComponentInputs{DeploymentMode: constants.RawDeployment},
		&v1beta1.EngineSpec{},
	).(*Engine)

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so the Get observes a closed ctx immediately

	done := make(chan struct{})
	var name string
	var err error
	go func() {
		defer close(done)
		name, err = engine.determineEngineName(ctx, isvc)
	}()

	select {
	case <-done:
		// Function returned promptly under canceled ctx. The ctx is
		// wired through to the client.Get call — pinning the signature
		// shape is the primary regression guard.
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(name).To(gomega.Equal("test-isvc-engine"))
	case <-time.After(2 * time.Second):
		t.Fatal("determineEngineName did not return promptly under canceled ctx")
	}
}

// TestEngineReconcileOMENativeSubresources_Rank0Selector pins that the
// per-Component stable Service selector ADDS `ome.io/pod-ordinal=0`
// when the Engine is multi-pod (Worker.Size > 0), but leaves the
// selector broad for single-pod Engines (so SurgeThenDrain's ordinal
// alternation doesn't zero-endpoint the Service during surge).
//
// Also pins that the PodMonitor selector NEVER carries the rank-0
// filter — Prometheus scrapes every pod of the gang, not just the
// leader. Workers emit per-rank metrics too.
func TestEngineReconcileOMENativeSubresources_Rank0Selector(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name                  string
		workerSize            *int
		wantStableSelectorHas string
	}{
		{
			name:                  "single-pod (no Worker) → no pod-ordinal filter",
			workerSize:            nil,
			wantStableSelectorHas: "",
		},
		{
			name:                  "multi-pod (Worker.Size=2) → pod-ordinal=0 added",
			workerSize:            intPtr(2),
			wantStableSelectorHas: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engineSpec := &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{Name: "engine", Image: "engine:v1"},
					},
				},
			}
			if tt.workerSize != nil {
				engineSpec.Leader = &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "engine", Image: "engine:v1"}},
					},
				}
				engineSpec.Worker = &v1beta1.WorkerSpec{
					Size: tt.workerSize,
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "engine", Image: "engine:v1"}},
					},
				}
			}

			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-isvc", Namespace: "default",
					UID: types.UID("test-isvc-uid"),
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine: engineSpec,
				},
			}
			objectMeta := metav1.ObjectMeta{
				Name:      "test-isvc-engine",
				Namespace: "default",
			}
			podSpec := &v1.PodSpec{
				Containers: []v1.Container{{Name: "engine", Image: "engine:v1",
					Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
			}

			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			clientset := fake.NewClientset()
			engine := NewEngine(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{DeploymentMode: constants.OMENative},
				engineSpec,
			).(*Engine)

			err := engine.reconcileOMENativeSubresources(context.Background(), isvc, objectMeta, podSpec)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Read back the stable Service and verify its selector.
			svc := &v1.Service{}
			g.Expect(c.Get(context.Background(),
				client.ObjectKey{Namespace: "default", Name: "test-isvc-engine"},
				svc)).To(gomega.Succeed())
			// Three base keys always present.
			g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
				constants.InferenceServicePodLabelKey, "test-isvc"))
			g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
				constants.OMEComponentLabel, string(v1beta1.EngineComponent)))
			g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
				query.LabelManagedBy, query.ManagedByOMENative))
			// Rank-0 filter (runner=leader + pod-ordinal=0) present only when
			// multi-pod. runner=leader is required because pod-ordinal is
			// numbered per runner, so the rank-0 worker also carries ordinal 0.
			if tt.wantStableSelectorHas == "" {
				g.Expect(svc.Spec.Selector).NotTo(gomega.HaveKey(query.LabelPodOrdinal),
					"single-pod Service selector must NOT carry %s", query.LabelPodOrdinal)
				g.Expect(svc.Spec.Selector).NotTo(gomega.HaveKey(query.LabelRunner),
					"single-pod Service selector must NOT carry %s", query.LabelRunner)
			} else {
				g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
					query.LabelPodOrdinal, tt.wantStableSelectorHas),
					"multi-pod Service selector must carry %s=%s",
					query.LabelPodOrdinal, tt.wantStableSelectorHas)
				g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
					query.LabelRunner, string(v1beta1.RunnerNameLeader)),
					"multi-pod Service selector must carry %s=leader so it can't match a worker",
					query.LabelRunner)
			}

			// PodMonitor selector NEVER carries the rank-0 filter —
			// Prometheus scrapes per pod, not per load-balancer endpoint.
			pm := &monitoringv1.PodMonitor{}
			g.Expect(c.Get(context.Background(),
				client.ObjectKey{Namespace: "default", Name: "test-isvc-engine"},
				pm)).To(gomega.Succeed())
			g.Expect(pm.Spec.Selector.MatchLabels).NotTo(gomega.HaveKey(query.LabelPodOrdinal),
				"PodMonitor selector must NEVER carry %s (workers emit metrics too)",
				query.LabelPodOrdinal)
			g.Expect(pm.Spec.Selector.MatchLabels).NotTo(gomega.HaveKey(query.LabelRunner),
				"PodMonitor selector must NEVER carry %s (workers emit metrics too)",
				query.LabelRunner)
		})
	}
}

// TestEngineReconcileOMENativeSubresources_ExtraEndpoints pins that the
// prometheus.ome.io/extra-endpoints annotation appends additional scrape
// endpoints to the OMENative PodMonitor, after the default /metrics endpoint.
// Mirrors an HTTP-router pod: a "metrics" port scraped at /metrics plus an
// "http" port whose /engine_metrics surface is declared via the annotation.
func TestEngineReconcileOMENativeSubresources_ExtraEndpoints(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	engineSpec := &v1beta1.EngineSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []v1.Container{{Name: "engine", Image: "engine:v1"}},
		},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-isvc", Namespace: "default",
			UID: types.UID("test-isvc-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{Engine: engineSpec},
	}
	objectMeta := metav1.ObjectMeta{
		Name:      "test-isvc-engine",
		Namespace: "default",
		Annotations: map[string]string{
			constants.ExtraPodMetricsEndpointsAnnotationKey: "http:/engine_metrics",
		},
	}
	// Router-shaped pod: "metrics" port scraped by default, "http" port carries
	// the annotation-declared /engine_metrics surface.
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{{Name: "engine", Image: "engine:v1",
			Ports: []v1.ContainerPort{
				{Name: "metrics", ContainerPort: 29000},
				{Name: "http", ContainerPort: 8000},
			}}},
	}

	c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	clientset := fake.NewClientset()
	engine := NewEngine(
		&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
		ComponentInputs{DeploymentMode: constants.OMENative},
		engineSpec,
	).(*Engine)

	err := engine.reconcileOMENativeSubresources(context.Background(), isvc, objectMeta, podSpec)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	pm := &monitoringv1.PodMonitor{}
	g.Expect(c.Get(context.Background(),
		client.ObjectKey{Namespace: "default", Name: "test-isvc-engine"},
		pm)).To(gomega.Succeed())

	g.Expect(pm.Spec.PodMetricsEndpoints).To(gomega.HaveLen(2))
	// Default /metrics endpoint stays first, on the "metrics" port.
	g.Expect(*pm.Spec.PodMetricsEndpoints[0].Port).To(gomega.Equal("metrics"))
	g.Expect(pm.Spec.PodMetricsEndpoints[0].Path).To(gomega.Equal("/metrics"))
	// Annotation-declared endpoint is appended after it.
	g.Expect(*pm.Spec.PodMetricsEndpoints[1].Port).To(gomega.Equal("http"))
	g.Expect(pm.Spec.PodMetricsEndpoints[1].Path).To(gomega.Equal("/engine_metrics"))
}

// TestEngineReconcileOMENativeSubresources_ManagedScrapeConfig pins the external-allocator
// wiring: the podMonitor config block (metadata labels + endpoint relabelings)
// is applied to the generated PodMonitor. The label goes to metadata.labels so
// a label-selecting collector scrapes it, but must NOT leak into spec.selector
// (pods don't carry it); the relabelings are appended to every endpoint so
// scraped series carry the dashboard label schema.
func TestEngineReconcileOMENativeSubresources_ManagedScrapeConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	engineSpec := &v1beta1.EngineSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []v1.Container{{Name: "engine", Image: "engine:v1"}},
		},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-isvc", Namespace: "default",
			UID: types.UID("test-isvc-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{Engine: engineSpec},
	}
	objectMeta := metav1.ObjectMeta{
		Name:      "test-isvc-engine",
		Namespace: "default",
		Annotations: map[string]string{
			constants.ExtraPodMetricsEndpointsAnnotationKey: "http:/engine_metrics",
		},
	}
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{{Name: "engine", Image: "engine:v1",
			Ports: []v1.ContainerPort{
				{Name: "metrics", ContainerPort: 29000},
				{Name: "http", ContainerPort: 8000},
			}}},
	}

	cfg := &controllerconfig.InferenceServicesConfig{
		PodMonitor: controllerconfig.PodMonitorConfig{
			Labels: map[string]string{"scrape.example.com/tier": "cluster"},
			Relabelings: []controllerconfig.RelabelConfig{
				{
					SourceLabels: []string{"__meta_kubernetes_pod_label_ome_io_inferenceservice"},
					TargetLabel:  "inferenceservice",
				},
				{
					SourceLabels: []string{"__meta_kubernetes_namespace"},
					TargetLabel:  "k8s_namespace_name",
				},
			},
		},
	}

	c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	clientset := fake.NewClientset()
	engine := NewEngine(
		&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: cfg},
		ComponentInputs{DeploymentMode: constants.OMENative},
		engineSpec,
	).(*Engine)

	err := engine.reconcileOMENativeSubresources(context.Background(), isvc, objectMeta, podSpec)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	pm := &monitoringv1.PodMonitor{}
	g.Expect(c.Get(context.Background(),
		client.ObjectKey{Namespace: "default", Name: "test-isvc-engine"},
		pm)).To(gomega.Succeed())

	// The allocator selection label is on metadata.labels...
	g.Expect(pm.Labels).To(gomega.HaveKeyWithValue("scrape.example.com/tier", "cluster"))
	// ...but NOT on spec.selector (would break pod matching).
	g.Expect(pm.Spec.Selector.MatchLabels).NotTo(gomega.HaveKey("scrape.example.com/tier"))
	g.Expect(pm.Spec.Selector.MatchLabels).To(gomega.HaveKeyWithValue(constants.OMEComponentLabel, string(v1beta1.EngineComponent)))

	// Relabelings appended to every endpoint (default + extra).
	g.Expect(pm.Spec.PodMetricsEndpoints).To(gomega.HaveLen(2))
	for _, ep := range pm.Spec.PodMetricsEndpoints {
		g.Expect(ep.RelabelConfigs).To(gomega.HaveLen(2))
		g.Expect(ep.RelabelConfigs[0].TargetLabel).To(gomega.Equal("inferenceservice"))
		g.Expect(ep.RelabelConfigs[0].SourceLabels).To(gomega.ContainElement(monitoringv1.LabelName("__meta_kubernetes_pod_label_ome_io_inferenceservice")))
		g.Expect(ep.RelabelConfigs[1].TargetLabel).To(gomega.Equal("k8s_namespace_name"))
	}
}

func TestEngineResourceMerging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                  string
		engineSpec            *v1beta1.EngineSpec
		runtime               *v1beta1.ServingRuntimeSpec
		acceleratorClass      *v1beta1.AcceleratorClassSpec
		expectResourcesMerged bool
		validateResources     func(*v1.Container)
	}{
		{
			name: "User specified resources - should NOT merge",
			engineSpec: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "ome-container",
						Image: "engine:latest",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("2"),
								v1.ResourceMemory: resource.MustParse("4Gi"),
							},
						},
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("4"),
									v1.ResourceMemory: resource.MustParse("8Gi"),
								},
							},
						},
					},
				},
			},
			expectResourcesMerged: false,
			validateResources: func(c *v1.Container) {
				// Should keep user's values (2 CPU, 4Gi memory)
				cpu := c.Resources.Requests[v1.ResourceCPU]
				g.Expect(cpu.String()).To(gomega.Equal("2"))
				memory := c.Resources.Requests[v1.ResourceMemory]
				g.Expect(memory.String()).To(gomega.Equal("4Gi"))
			},
		},
		{
			name: "User did NOT specify resources - should merge from runtime",
			engineSpec: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "ome-container",
						Image: "engine:latest",
						// No resources specified
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("4"),
									v1.ResourceMemory: resource.MustParse("8Gi"),
								},
							},
						},
					},
				},
			},
			expectResourcesMerged: true,
			validateResources: func(c *v1.Container) {
				// Should use runtime's values (4 CPU, 8Gi memory)
				cpu := c.Resources.Requests[v1.ResourceCPU]
				g.Expect(cpu.String()).To(gomega.Equal("4"))
				memory := c.Resources.Requests[v1.ResourceMemory]
				g.Expect(memory.String()).To(gomega.Equal("8Gi"))
			},
		},
		{
			name: "User did NOT specify resources - should merge from AcceleratorClass",
			engineSpec: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "ome-container",
						Image: "engine:latest",
						// No resources specified
					},
				},
			},
			acceleratorClass: &v1beta1.AcceleratorClassSpec{
				Resources: []v1beta1.AcceleratorResource{
					{
						Name:     "nvidia.com/gpu",
						Quantity: resource.MustParse("2"),
					},
				},
			},
			expectResourcesMerged: true,
			validateResources: func(c *v1.Container) {
				// Should have GPU from AC
				gpu := c.Resources.Requests[v1.ResourceName("nvidia.com/gpu")]
				g.Expect(gpu.String()).To(gomega.Equal("2"))
			},
		},
		{
			name: "No runner specified - should NOT merge",
			engineSpec: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "ome-container",
							Image: "engine:latest",
						},
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name: "ome-container",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("4"),
									v1.ResourceMemory: resource.MustParse("8Gi"),
								},
							},
						},
					},
				},
			},
			expectResourcesMerged: false,
			validateResources: func(c *v1.Container) {
				// Should not have resources merged
				g.Expect(c.Resources.Requests).To(gomega.BeNil())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model:  &v1beta1.ModelRef{},
					Engine: tt.engineSpec,
				},
			}

			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			engine := NewEngine(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode:       constants.RawDeployment,
					Runtime:              tt.runtime,
					RuntimeName:          "test-runtime",
					AcceleratorClass:     tt.acceleratorClass,
					AcceleratorClassName: "test-accel-class",
				},
				tt.engineSpec,
			).(*Engine)

			// Call reconcilePodSpec which internally calls MergeEngineResources
			objectMeta := &metav1.ObjectMeta{Name: "test", Namespace: "default"}
			podSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Find the runner container
			var runnerContainer *v1.Container
			for i := range podSpec.Containers {
				if podSpec.Containers[i].Name == "ome-container" {
					runnerContainer = &podSpec.Containers[i]
					break
				}
			}
			g.Expect(runnerContainer).NotTo(gomega.BeNil())

			// Validate resources
			if tt.validateResources != nil {
				tt.validateResources(runnerContainer)
			}
		})
	}
}

func TestEngineAffinityMerging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                 string
		engineSpec           *v1beta1.EngineSpec
		acceleratorClass     *v1beta1.AcceleratorClassSpec
		expectAffinityMerged bool
		validateAffinity     func(*v1.Affinity)
	}{
		{
			name: "User specified affinity - should NOT merge",
			engineSpec: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "custom-key",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"custom-value"},
											},
										},
									},
								},
							},
						},
					},
					Containers: []v1.Container{
						{Name: "ome-container", Image: "engine:latest"},
					},
				},
			},
			acceleratorClass: &v1beta1.AcceleratorClassSpec{
				Discovery: v1beta1.AcceleratorDiscovery{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "ac-key",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"ac-value"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectAffinityMerged: false,
			validateAffinity: func(affinity *v1.Affinity) {
				// Should keep user's affinity (custom-key)
				g.Expect(affinity).NotTo(gomega.BeNil())
				g.Expect(affinity.NodeAffinity).NotTo(gomega.BeNil())
				terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
				g.Expect(terms[0].MatchExpressions[0].Key).To(gomega.Equal("custom-key"))
			},
		},
		{
			name: "User did NOT specify affinity - should merge from AC",
			engineSpec: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					// No affinity specified
					Containers: []v1.Container{
						{Name: "ome-container", Image: "engine:latest"},
					},
				},
			},
			acceleratorClass: &v1beta1.AcceleratorClassSpec{
				Discovery: v1beta1.AcceleratorDiscovery{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "ac-key",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"ac-value"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectAffinityMerged: true,
			validateAffinity: func(affinity *v1.Affinity) {
				// Should use AC's affinity (ac-key)
				g.Expect(affinity).NotTo(gomega.BeNil())
				g.Expect(affinity.NodeAffinity).NotTo(gomega.BeNil())
				terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
				g.Expect(terms[0].MatchExpressions[0].Key).To(gomega.Equal("ac-key"))
			},
		},
		{
			name: "No affinity from AC - should remain nil",
			engineSpec: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{Name: "ome-container", Image: "engine:latest"},
					},
				},
			},
			acceleratorClass:     nil,
			expectAffinityMerged: false,
			validateAffinity: func(affinity *v1.Affinity) {
				// Should be nil
				g.Expect(affinity).To(gomega.BeNil())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model:  &v1beta1.ModelRef{},
					Engine: tt.engineSpec,
				},
			}

			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			engine := NewEngine(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode:       constants.RawDeployment,
					RuntimeName:          "test-runtime",
					AcceleratorClass:     tt.acceleratorClass,
					AcceleratorClassName: "test-accel-class",
				},
				tt.engineSpec,
			).(*Engine)

			// Call reconcilePodSpec which internally calls UpdateEngineAffinity
			objectMeta := &metav1.ObjectMeta{Name: "test", Namespace: "default"}
			podSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Validate affinity
			if tt.validateAffinity != nil {
				tt.validateAffinity(podSpec.Affinity)
			}
		})
	}
}

// Note: Worker resource and affinity tests are not included because MergeEngineResources and
// UpdateEngineAffinity check isvc.Spec.Engine.Runner and isvc.Spec.Engine.PodSpec.Affinity,
// not the worker-specific fields. This means the merging decision is based on the engine/leader
// spec, not the worker spec. This is the current implementation behavior.
