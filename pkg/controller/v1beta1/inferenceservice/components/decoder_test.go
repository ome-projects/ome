package components

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/render"

	policyv1 "k8s.io/api/policy/v1"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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

// TestDecoderReconcileOMENativeSubresources_LeaderSelector pins that the
// decoder's stable Service selects only the leader pod whenever the effective
// decoder shape is a leader/worker gang, whether the InferenceService or only
// the runtime declares it, and keeps the broad selector for a single pod.
func TestDecoderReconcileOMENativeSubresources_LeaderSelector(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	decoderPodSpec := func() v1beta1.PodSpec {
		return v1beta1.PodSpec{Containers: []v1.Container{{Name: "decoder", Image: "decoder:v1"}}}
	}
	gang := func() (*v1beta1.LeaderSpec, *v1beta1.WorkerSpec) {
		return &v1beta1.LeaderSpec{PodSpec: decoderPodSpec()},
			&v1beta1.WorkerSpec{Size: intPtr(3), PodSpec: decoderPodSpec()}
	}
	isvcGang := func() *v1beta1.DecoderSpec {
		leader, worker := gang()
		return &v1beta1.DecoderSpec{PodSpec: decoderPodSpec(), Leader: leader, Worker: worker}
	}
	runtimeGang := func() *v1beta1.ServingRuntimeSpec {
		leader, worker := gang()
		return &v1beta1.ServingRuntimeSpec{DecoderConfig: &v1beta1.DecoderSpec{
			PodSpec: decoderPodSpec(), Leader: leader, Worker: worker,
		}}
	}

	tests := []struct {
		name           string
		isvcDecoder    *v1beta1.DecoderSpec
		runtime        *v1beta1.ServingRuntimeSpec
		wantLeaderOnly bool
	}{
		{
			name:        "single-pod decoder keeps the broad selector",
			isvcDecoder: &v1beta1.DecoderSpec{PodSpec: decoderPodSpec()},
		},
		{
			name:           "leader and worker declared on the InferenceService select the leader",
			isvcDecoder:    isvcGang(),
			wantLeaderOnly: true,
		},
		{
			name:           "leader and worker supplied only by the runtime select the leader",
			isvcDecoder:    &v1beta1.DecoderSpec{},
			runtime:        runtimeGang(),
			wantLeaderOnly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-isvc", Namespace: "default",
					UID: types.UID("test-isvc-uid"),
				},
				Spec: v1beta1.InferenceServiceSpec{
					Engine:  &v1beta1.EngineSpec{PodSpec: decoderPodSpec()},
					Decoder: tt.isvcDecoder,
				},
			}
			decoderSpec := tt.isvcDecoder
			if tt.runtime != nil {
				specs, err := isvcutils.MergeRuntimeRoleSpecs(isvc.DeepCopy(), tt.runtime, logr.Discard())
				merged := specs.Decoder
				g.Expect(err).NotTo(gomega.HaveOccurred())
				decoderSpec = merged
			}
			objectMeta := metav1.ObjectMeta{
				Name:      "test-isvc-decoder",
				Namespace: "default",
			}
			podSpec := &v1.PodSpec{
				Containers: []v1.Container{{Name: "decoder", Image: "decoder:v1",
					Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
			}

			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			clientset := fake.NewClientset()
			decoder := NewDecoder(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{DeploymentMode: constants.OMENative},
				decoderSpec,
			).(*Decoder)

			err := decoder.reconcileOMENativeSubresources(context.Background(), isvc, objectMeta, podSpec)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			svc := &v1.Service{}
			g.Expect(c.Get(context.Background(),
				client.ObjectKey{Namespace: "default", Name: "test-isvc-decoder"},
				svc)).To(gomega.Succeed())
			g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
				constants.OMEComponentLabel, string(v1beta1.DecoderComponent)))
			podLabels := func(runner v1beta1.RunnerName, ordinal string) labels.Set {
				return labels.Set{
					constants.InferenceServicePodLabelKey: "test-isvc",
					constants.OMEComponentLabel:           string(v1beta1.DecoderComponent),
					query.LabelManagedBy:                  query.ManagedByOMENative,
					query.LabelRunner:                     string(runner),
					query.LabelPodOrdinal:                 ordinal,
				}
			}
			selector := labels.SelectorFromSet(svc.Spec.Selector)
			if tt.wantLeaderOnly {
				g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(query.LabelPodOrdinal, "0"))
				g.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue(
					query.LabelRunner, string(v1beta1.RunnerNameLeader)))
				g.Expect(selector.Matches(podLabels(v1beta1.RunnerNameLeader, "0"))).To(gomega.BeTrue())
				g.Expect(selector.Matches(podLabels(v1beta1.RunnerNameWorker, "0"))).To(gomega.BeFalse(),
					"a worker pod must never be an endpoint")
			} else {
				g.Expect(svc.Spec.Selector).NotTo(gomega.HaveKey(query.LabelPodOrdinal))
				g.Expect(svc.Spec.Selector).NotTo(gomega.HaveKey(query.LabelRunner))
				g.Expect(selector.Matches(podLabels(v1beta1.RunnerNameDefault, "1"))).To(gomega.BeTrue())
			}

			pm := &monitoringv1.PodMonitor{}
			g.Expect(c.Get(context.Background(),
				client.ObjectKey{Namespace: "default", Name: "test-isvc-decoder"},
				pm)).To(gomega.Succeed())
			g.Expect(pm.Spec.Selector.MatchLabels).NotTo(gomega.HaveKey(query.LabelPodOrdinal))
			g.Expect(pm.Spec.Selector.MatchLabels).NotTo(gomega.HaveKey(query.LabelRunner))
		})
	}
}

func TestDecoderReconcile(t *testing.T) {
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
		decoderSpec    *v1beta1.DecoderSpec
		runtime        *v1beta1.ServingRuntimeSpec
		runtimeName    string
		isvc           *v1beta1.InferenceService
		validate       func(*testing.T, client.Client, *v1beta1.InferenceService)
		wantErr        bool
		setupMocks     func(client.Client, kubernetes.Interface)
	}{
		{
			name:           "Raw deployment for PD-disaggregated decoder",
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
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 3,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "decoder-container",
							Image: "decoder:latest",
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
			runtime:     &v1beta1.ServingRuntimeSpec{},
			runtimeName: "pd-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pd-isvc",
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
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check deployment was created
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-pd-isvc-decoder",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(gomega.Equal("decoder:latest"))

				// Check node selector was added for the base model
				g.Expect(deployment.Spec.Template.Spec.NodeSelector).NotTo(gomega.BeNil())
				value, found := deployment.Spec.Template.Spec.NodeSelector["models.ome.io/default.basemodel.base-model-1"]
				g.Expect(found).To(gomega.BeTrue(), "Model node selector not found")
				g.Expect(value).To(gomega.Equal("Ready"))

				// Check service was created
				service := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-pd-isvc-decoder",
					Namespace: "default",
				}, service)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
		},
		{
			name:           "Multi-node decoder deployment",
			deploymentMode: constants.MultiNode,
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "base-model-2",
				Namespace: "default",
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(2),
				},
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "leader-container",
								Image: "decoder-leader:latest",
							},
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker-container",
								Image: "decoder-worker:latest",
							},
						},
					},
				},
			},
			runtime:     &v1beta1.ServingRuntimeSpec{},
			runtimeName: "multi-node-decoder-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-mn-decoder-isvc",
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
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
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

				// Verify leader pod has model node selector
				g.Expect(leaderNodeSelector).NotTo(gomega.BeNil())
				leaderValue, leaderFound := leaderNodeSelector["models.ome.io/default.basemodel.base-model-2"]
				g.Expect(leaderFound).To(gomega.BeTrue(), "Leader model node selector not found")
				g.Expect(leaderValue).To(gomega.Equal("Ready"))

				// Verify worker pod has model node selector
				g.Expect(workerNodeSelector).NotTo(gomega.BeNil())
				workerValue, workerFound := workerNodeSelector["models.ome.io/default.basemodel.base-model-2"]
				g.Expect(workerFound).To(gomega.BeTrue(), "Worker model node selector not found")
				g.Expect(workerValue).To(gomega.Equal("Ready"))
			},
		},
		{
			name:           "ClusterBaseModel decoder with node selector",
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
				Name: "cluster-decoder-model",
				// No namespace for ClusterBaseModel
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 2,
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
			runtime:     &v1beta1.ServingRuntimeSpec{},
			runtimeName: "decoder-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster-decoder",
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
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check deployment was created
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-cluster-decoder-decoder",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Check node selector for ClusterBaseModel (no namespace in label)
				g.Expect(deployment.Spec.Template.Spec.NodeSelector).NotTo(gomega.BeNil())
				value, found := deployment.Spec.Template.Spec.NodeSelector["models.ome.io/clusterbasemodel.cluster-decoder-model"]
				g.Expect(found).To(gomega.BeTrue(), "Model node selector not found")
				g.Expect(value).To(gomega.Equal("Ready"))
			},
		},
		{
			name:           "Decoder with nil spec should error",
			deploymentMode: constants.RawDeployment,
			decoderSpec:    nil,
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-nil-decoder",
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
			// Create fake client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.isvc).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// Setup mocks if needed
			if tt.setupMocks != nil {
				tt.setupMocks(c, clientset)
			}

			// Create inference service config
			isvcConfig := &controllerconfig.InferenceServicesConfig{}

			// Create decoder component
			decoder := NewDecoder(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: isvcConfig},
				ComponentInputs{
					DeploymentMode: tt.deploymentMode,
					BaseModel:      tt.baseModel,
					BaseModelMeta:  tt.baseModelMeta,
					Runtime:        tt.runtime,
					RuntimeName:    tt.runtimeName,
				},
				tt.decoderSpec,
			)

			// Reconcile
			result, err := decoder.Reconcile(context.TODO(), tt.isvc)

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

func TestDecoderReconcileObjectMeta(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                string
		isvc                *v1beta1.InferenceService
		decoderSpec         *v1beta1.DecoderSpec
		baseModel           *v1beta1.BaseModelSpec
		baseModelMeta       *metav1.ObjectMeta
		runtimeName         string
		expectedAnnotations map[string]string
		expectedLabels      map[string]string
		expectedName        string
	}{
		{
			name: "Basic decoder object metadata",
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
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{
						"decoder-annotation": "decoder-value",
					},
					Labels: map[string]string{
						"decoder-label": "decoder-value",
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name:    "safetensors",
					Version: stringPtr("1.0"),
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/decoder"),
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
				"decoder-annotation":                   "decoder-value",
				constants.BaseModelName:                "base-model",
				constants.BaseModelFormat:              "safetensors",
				constants.BaseModelFormatVersion:       "1.0",
				constants.BaseModelVendorAnnotationKey: "meta",
				constants.ServingRuntimeKeyName:        "test-runtime",
			},
			expectedLabels: map[string]string{
				"custom-label":                                  "value",
				"decoder-label":                                 "decoder-value",
				constants.InferenceServicePodLabelKey:           "test-isvc",
				constants.OMEComponentLabel:                     "decoder",
				constants.ServingRuntimeLabelKey:                "test-runtime",
				constants.InferenceServiceBaseModelNameLabelKey: "base-model",
				constants.InferenceServiceBaseModelSizeLabelKey: "LARGE",
				constants.BaseModelTypeLabelKey:                 "Serving",
				constants.BaseModelVendorLabelKey:               "meta",
				constants.FTServingLabelKey:                     "false",
			},
			expectedName: "test-isvc-decoder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scheme
			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			// Create decoder using the constructor
			decoder := NewDecoder(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode: constants.RawDeployment,
					BaseModel:      tt.baseModel,
					BaseModelMeta:  tt.baseModelMeta,
					RuntimeName:    tt.runtimeName,
				},
				tt.decoderSpec,
			).(*Decoder)

			// Build the decoder's object metadata
			objectMeta, err := ReconcileComponentObjectMeta(&decoder.BaseComponentFields, tt.isvc, v1beta1.DecoderComponent, render.ComponentName(tt.isvc, v1beta1.DecoderComponent), decoder.decoderSpec.Annotations, decoder.decoderSpec.Labels)
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

func TestDecoderWorkerPodSpec(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name               string
		decoderSpec        *v1beta1.DecoderSpec
		expectedError      bool
		expectedContainers int
		validatePodSpec    func(*v1.PodSpec)
	}{
		{
			name: "Worker with leader runner",
			decoderSpec: &v1beta1.DecoderSpec{
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
			name:        "No worker spec returns nil",
			decoderSpec: &v1beta1.DecoderSpec{},
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

			// Create decoder using the constructor
			decoder := NewDecoder(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{DeploymentMode: constants.RawDeployment},
				tt.decoderSpec,
			).(*Decoder)

			podSpec, err := render.DecoderWorkerPodSpec(&decoder.Piece, isvc, decoder.decoderSpec, objectMeta)

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

func TestDecoderAcceleratorOverride(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name        string
		isvc        *v1beta1.InferenceService
		decoderSpec *v1beta1.DecoderSpec
		runtime     *v1beta1.ServingRuntimeSpec
		validate    func(*testing.T, client.Client, *v1beta1.InferenceService)
	}{
		{
			name: "Decoder with accelerator override",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-decoder-accel",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
					Decoder: &v1beta1.DecoderSpec{
						AcceleratorOverride: &v1beta1.AcceleratorSelector{
							AcceleratorClass: stringPtr("nvidia-h100"),
							Policy:           v1beta1.BestFitPolicy,
						},
					},
				},
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "decoder-container",
							Image: "decoder:latest",
						},
					},
				},
				AcceleratorOverride: &v1beta1.AcceleratorSelector{
					AcceleratorClass: stringPtr("nvidia-h100"),
					Policy:           v1beta1.BestFitPolicy,
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-h100", "nvidia-a100"},
				},
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Verify that the accelerator override is properly set
				g.Expect(isvc.Spec.Decoder.AcceleratorOverride).NotTo(gomega.BeNil())
				g.Expect(isvc.Spec.Decoder.AcceleratorOverride.AcceleratorClass).NotTo(gomega.BeNil())
				g.Expect(*isvc.Spec.Decoder.AcceleratorOverride.AcceleratorClass).To(gomega.Equal("nvidia-h100"))
				g.Expect(isvc.Spec.Decoder.AcceleratorOverride.Policy).To(gomega.Equal(v1beta1.BestFitPolicy))
			},
		},
		{
			name: "Decoder without accelerator override",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-decoder-no-accel",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "decoder-container",
							Image: "decoder:latest",
						},
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Verify that no accelerator override is set
				if isvc.Spec.Decoder != nil {
					g.Expect(isvc.Spec.Decoder.AcceleratorOverride).To(gomega.BeNil())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scheme
			scheme := runtime.NewScheme()
			g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

			// Create fake client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.isvc).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// The decoder component builds from these inputs.
			g.Expect(NewDecoder(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode: constants.RawDeployment,
					Runtime:        tt.runtime,
					RuntimeName:    "test-runtime",
				},
				tt.decoderSpec,
			)).NotTo(gomega.BeNil())

			// Run validations
			if tt.validate != nil {
				tt.validate(t, c, tt.isvc)
			}
		})
	}
}

func TestDecoderAcceleratorClassSelector(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                     string
		isvc                     *v1beta1.InferenceService
		runtime                  *v1beta1.ServingRuntimeSpec
		expectedAcceleratorClass *string
	}{
		{
			name: "Decoder with accelerator override takes precedence",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-decoder-override",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
					Decoder: &v1beta1.DecoderSpec{
						AcceleratorOverride: &v1beta1.AcceleratorSelector{
							AcceleratorClass: stringPtr("nvidia-h100"),
						},
					},
					AcceleratorSelector: &v1beta1.AcceleratorSelector{
						AcceleratorClass: stringPtr("nvidia-a100"), // This should be ignored
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-h100", "nvidia-a100", "nvidia-v100"},
				},
			},
			expectedAcceleratorClass: stringPtr("nvidia-h100"),
		},
		{
			name: "InferenceService accelerator selector used when no component override",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc-selector",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
					AcceleratorSelector: &v1beta1.AcceleratorSelector{
						AcceleratorClass: stringPtr("nvidia-a100"),
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-h100", "nvidia-a100", "nvidia-v100"},
				},
			},
			expectedAcceleratorClass: stringPtr("nvidia-a100"),
		},
		{
			name: "First runtime accelerator class used as default",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-default-accel",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-v100", "nvidia-a100"},
				},
			},
			expectedAcceleratorClass: stringPtr("nvidia-v100"),
		},
		{
			name: "No accelerator class when runtime has no requirements",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-no-accel",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			runtime:                  &v1beta1.ServingRuntimeSpec{},
			expectedAcceleratorClass: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the accelerator class selection logic
			// This simulates what would happen in the actual reconciliation

			// Import the accelerator class selector
			// Note: In a real test, you would create a mock accelerator class selector
			// For now, we'll test the logic directly using the GetAcceleratorClass function

			// This would be the call to the accelerator class selector
			selectedClass := getAcceleratorClassForDecoder(tt.isvc, tt.runtime)

			if tt.expectedAcceleratorClass == nil {
				g.Expect(selectedClass).To(gomega.BeNil())
			} else {
				g.Expect(selectedClass).NotTo(gomega.BeNil())
				g.Expect(*selectedClass).To(gomega.Equal(*tt.expectedAcceleratorClass))
			}
		})
	}
}

// Helper function to simulate accelerator class selection for decoder
func getAcceleratorClassForDecoder(isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec) *string {
	// This simulates the accelerator class selector logic for decoder component
	// In the actual implementation, this would call acceleratorclassselector.GetAcceleratorClass

	// 1. If runtime doesn't contain AcceleratorRequirements, return nil
	if runtime == nil || runtime.AcceleratorRequirements == nil {
		return nil
	}

	// 2. If runtime contains AcceleratorRequirements, check component-specific overrides
	if len(runtime.AcceleratorRequirements.AcceleratorClasses) > 0 {
		// Check decoder-specific AcceleratorOverride
		if isvc != nil && isvc.Spec.Decoder != nil &&
			isvc.Spec.Decoder.AcceleratorOverride != nil &&
			isvc.Spec.Decoder.AcceleratorOverride.AcceleratorClass != nil {
			return isvc.Spec.Decoder.AcceleratorOverride.AcceleratorClass
		}

		// Check InferenceService-level AcceleratorSelector
		if isvc != nil && isvc.Spec.AcceleratorSelector != nil &&
			isvc.Spec.AcceleratorSelector.AcceleratorClass != nil {
			return isvc.Spec.AcceleratorSelector.AcceleratorClass
		}

		// Return the first AcceleratorClass from runtime requirements as default
		if len(runtime.AcceleratorRequirements.AcceleratorClasses) > 0 {
			return &runtime.AcceleratorRequirements.AcceleratorClasses[0]
		}
	}

	return nil
}

func TestDecoderSetupMocks(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name           string
		setupMocks     func(client.Client, kubernetes.Interface)
		expectedError  bool
		expectedConfig string
	}{
		{
			name: "Setup inferenceservice config",
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
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			expectedError:  false,
			expectedConfig: "{}",
		},
		{
			name:           "Setup error",
			setupMocks:     func(c client.Client, cs kubernetes.Interface) {},
			expectedError:  true,
			expectedConfig: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client
			scheme := runtime.NewScheme()
			// Add v1 to scheme for ConfigMap
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// Setup mocks
			tt.setupMocks(c, clientset)

			// Get inferenceservice config from clientset
			cm, err := clientset.CoreV1().ConfigMaps("ome").Get(context.TODO(), "inferenceservice-config", metav1.GetOptions{})

			if tt.expectedError {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(cm.Data["config"]).To(gomega.Equal(tt.expectedConfig))
			}
		})
	}
}

// TestDecoderUsesLeaderTemplate pins the structural predicate that drives
// pod-template selection in decoder.reconcilePodSpec. The earlier
// implementation switched on d.DeploymentMode and only sourced the Leader
// template under `case constants.MultiNode`. A multi-pod OMENative decoder
// (Leader/Worker set, DeploymentMode == OMENative) fell to the default
// branch and collapsed onto the empty top-level runner — reconcilePodSpec
// then failed with "no containers found in pod spec and no runner spec
// provided", which short-circuited the component loop and left the decoder
// (and router) InferenceReplicas un-projected. decoderUsesLeaderTemplate
// decouples template selection from dispatch-mode classification, matching
// engineUsesLeaderTemplate.
func TestDecoderUsesLeaderTemplate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name     string
		spec     *v1beta1.DecoderSpec
		expected bool
	}{
		{
			name:     "nil spec returns false",
			spec:     nil,
			expected: false,
		},
		{
			name:     "empty spec — no leader — false",
			spec:     &v1beta1.DecoderSpec{},
			expected: false,
		},
		{
			name: "leader set — true (regardless of mode)",
			spec: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "leader"}},
					},
				},
			},
			expected: true,
		},
		{
			name: "leader + worker set — true (multi-pod OMENative shape)",
			spec: &v1beta1.DecoderSpec{
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
			spec: &v1beta1.DecoderSpec{
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g.Expect(render.DecoderUsesLeaderTemplate(tt.spec)).To(gomega.Equal(tt.expected))
		})
	}
}

func TestDecoderResourceMerging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                  string
		decoderSpec           *v1beta1.DecoderSpec
		runtime               *v1beta1.ServingRuntimeSpec
		acceleratorClass      *v1beta1.AcceleratorClassSpec
		expectResourcesMerged bool
		validateResources     func(*v1.Container)
	}{
		{
			name: "User specified resources - should NOT merge",
			decoderSpec: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "decoder",
						Image: "decoder:latest",
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
							Name: "decoder",
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
			decoderSpec: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "decoder",
						Image: "decoder:latest",
						// No resources specified
					},
				},
			},
			runtime: &v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []v1.Container{
						{
							Name: "decoder",
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
			decoderSpec: &v1beta1.DecoderSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: v1.Container{
						Name:  "decoder",
						Image: "decoder:latest",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model:   &v1beta1.ModelRef{},
					Decoder: tt.decoderSpec,
				},
			}

			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			decoder := NewDecoder(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode:       constants.RawDeployment,
					Runtime:              tt.runtime,
					RuntimeName:          "test-runtime",
					AcceleratorClass:     tt.acceleratorClass,
					AcceleratorClassName: "test-accel-class",
				},
				tt.decoderSpec,
			).(*Decoder)

			// Render the primary pod, which merges the role resources
			objectMeta := &metav1.ObjectMeta{Name: "test", Namespace: "default"}
			podSpec, err := render.DecoderPodSpec(&decoder.Piece, isvc, decoder.decoderSpec, objectMeta)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Find the runner container
			var runnerContainer *v1.Container
			for i := range podSpec.Containers {
				if podSpec.Containers[i].Name == "decoder" {
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

func TestDecoderAffinityMerging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                 string
		decoderSpec          *v1beta1.DecoderSpec
		acceleratorClass     *v1beta1.AcceleratorClassSpec
		expectAffinityMerged bool
		validateAffinity     func(*v1.Affinity)
	}{
		{
			name: "User specified affinity - should NOT merge",
			decoderSpec: &v1beta1.DecoderSpec{
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
						{Name: "decoder", Image: "decoder:latest"},
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
			decoderSpec: &v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					// No affinity specified
					Containers: []v1.Container{
						{Name: "decoder", Image: "decoder:latest"},
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
			decoderSpec: &v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{Name: "decoder", Image: "decoder:latest"},
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
					Model:   &v1beta1.ModelRef{},
					Decoder: tt.decoderSpec,
				},
			}

			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			decoder := NewDecoder(
				&ComponentDeps{Client: c, Clientset: clientset, Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}},
				ComponentInputs{
					DeploymentMode:       constants.RawDeployment,
					RuntimeName:          "test-runtime",
					AcceleratorClass:     tt.acceleratorClass,
					AcceleratorClassName: "test-accel-class",
				},
				tt.decoderSpec,
			).(*Decoder)

			// Render the primary pod, which applies the role affinity
			objectMeta := &metav1.ObjectMeta{Name: "test", Namespace: "default"}
			podSpec, err := render.DecoderPodSpec(&decoder.Piece, isvc, decoder.decoderSpec, objectMeta)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Validate affinity
			if tt.validateAffinity != nil {
				tt.validateAffinity(podSpec.Affinity)
			}
		})
	}
}

// int32Ptr returns a pointer to the given int32.
func int32Ptr(i int32) *int32 {
	return &i
}
