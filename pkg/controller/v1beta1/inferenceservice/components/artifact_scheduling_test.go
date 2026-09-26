package components

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/deployment"
)

func TestSinglePodArtifactSelectorsOverrideAllLayers(t *testing.T) {
	for _, component := range []string{"engine", "decoder"} {
		for _, mode := range []constants.DeploymentModeType{constants.RawDeployment, constants.OMENative} {
			t.Run(component+"/"+string(mode), func(t *testing.T) {
				testArtifactSelectorLayers(t, component, mode, false)
			})
		}
	}
}

func testArtifactSelectorLayers(t *testing.T, component string, mode constants.DeploymentModeType, leader bool) {
	t.Helper()
	for _, layer := range []string{"pod", "runtime", "accelerator", "user"} {
		for _, request := range []string{"", "restore-001", "restore-002"} {
			t.Run(layer+"/"+request, func(t *testing.T) {
				meta := &metav1.ObjectMeta{Name: "model", Namespace: "models", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: request}}
				readyKey := constants.GetBaseModelLabel(meta.Namespace, meta.Name)
				requestKey := constants.GetModelArtifactRequestLabel(meta.UID)
				selectors := map[string]string{readyKey: "Failed", "unrelated": "retained"}
				if request != "" {
					selectors[requestKey] = "stale-request"
				}
				pod := v1beta1.PodSpec{Containers: []corev1.Container{{Name: "server", Image: "test"}}}
				input := ComponentInputs{BaseModel: &v1beta1.BaseModelSpec{}, BaseModelMeta: meta, DeploymentMode: mode}
				isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "models"}, Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{}}}
				switch layer {
				case "pod":
					pod.NodeSelector = selectors
				case "runtime":
					input.Runtime = &v1beta1.ServingRuntimeSpec{ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{NodeSelector: selectors}}
				case "accelerator":
					input.AcceleratorClass = &v1beta1.AcceleratorClassSpec{}
					input.AcceleratorClass.Discovery.NodeSelector = selectors
				case "user":
					isvc.Spec.Engine.NodeSelector = selectors
					isvc.Spec.Decoder.NodeSelector = selectors
				}
				actual := artifactSchedulingPod(t, component, input, pod, isvc, leader, false)
				require.Equal(t, "Ready", actual.NodeSelector[readyKey])
				require.Equal(t, request, actual.NodeSelector[requestKey])
				if request == "" {
					require.NotContains(t, actual.NodeSelector, requestKey)
				}
				require.Equal(t, "retained", actual.NodeSelector["unrelated"])
				require.Nil(t, isvc.Annotations, "scheduling must not add a request binding to the service")
				require.Equal(t, request, meta.Annotations[constants.ModelArtifactRehydrationIDAnnotation])
				if mode == constants.RawDeployment {
					rendered := deployment.NewDeploymentReconciler(nil, nil, isvc.ObjectMeta, &v1beta1.ComponentExtensionSpec{}, actual, nil).Deployment
					require.Equal(t, actual.NodeSelector, rendered.Spec.Template.Spec.NodeSelector)
				}
			})
		}
	}
}

func TestArtifactSelectorExclusions(t *testing.T) {
	for _, mode := range []constants.DeploymentModeType{constants.RawDeployment, constants.OMENative} {
		for _, component := range []string{"engine", "decoder"} {
			for _, exclusion := range []string{"absent", "pvc", "sharded", "merged", "none"} {
				t.Run(string(mode)+"/"+component+"/"+exclusion, func(t *testing.T) {
					meta := &metav1.ObjectMeta{Name: "model", UID: "uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-001"}}
					input := ComponentInputs{BaseModel: &v1beta1.BaseModelSpec{}, BaseModelMeta: meta, DeploymentMode: mode}
					switch exclusion {
					case "absent":
						input.BaseModel, input.BaseModelMeta = nil, nil
					case "pvc":
						uri := "pvc://claim/model"
						input.BaseModel.Storage = &v1beta1.StorageSpec{StorageUri: &uri}
					case "sharded":
						distribution := v1beta1.DistributionSharded
						input.BaseModel.Distribution = &distribution
					}
					pod := v1beta1.PodSpec{Containers: []corev1.Container{{Name: "server", Image: "test"}}, NodeSelector: map[string]string{"unrelated": "retained"}}
					isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "models"}}
					actual := artifactSchedulingPod(t, component, input, pod, isvc, false, exclusion == "merged")
					if exclusion == "none" {
						require.Equal(t, "Ready", actual.NodeSelector[constants.GetClusterBaseModelLabel(meta.Name)])
						require.Equal(t, "restore-001", actual.NodeSelector[constants.GetModelArtifactRequestLabel(meta.UID)])
					} else {
						require.NotContains(t, actual.NodeSelector, constants.GetClusterBaseModelLabel(meta.Name))
						require.NotContains(t, actual.NodeSelector, constants.GetModelArtifactRequestLabel(meta.UID))
					}
					require.Equal(t, "retained", actual.NodeSelector["unrelated"])
				})
			}
		}
	}
}

func artifactSchedulingPod(t *testing.T, component string, input ComponentInputs, pod v1beta1.PodSpec, isvc *v1beta1.InferenceService, leader, merged bool) *corev1.PodSpec {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	deps := &ComponentDeps{Client: ctrlfake.NewClientBuilder().WithScheme(scheme).Build(), Clientset: fake.NewClientset(), Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}}
	var actual *corev1.PodSpec
	var err error
	if strings.HasPrefix(component, "engine") {
		spec := &v1beta1.EngineSpec{PodSpec: pod}
		if leader {
			spec.Leader = &v1beta1.LeaderSpec{PodSpec: pod}
			spec.Worker = &v1beta1.WorkerSpec{PodSpec: pod}
		}
		engine := NewEngine(deps, input, spec).(*Engine)
		engine.FineTunedServingWithMergedWeights = merged
		if component == "engine-worker" {
			actual, err = engine.reconcileWorkerPodSpec(isvc, &isvc.ObjectMeta)
		} else {
			actual, err = engine.reconcilePodSpec(isvc, &isvc.ObjectMeta)
		}
	} else {
		spec := &v1beta1.DecoderSpec{PodSpec: pod}
		if leader {
			spec.Leader = &v1beta1.LeaderSpec{PodSpec: pod}
			spec.Worker = &v1beta1.WorkerSpec{PodSpec: pod}
		}
		decoder := NewDecoder(deps, input, spec).(*Decoder)
		decoder.FineTunedServingWithMergedWeights = merged
		if component == "decoder-worker" {
			actual, err = decoder.reconcileWorkerPodSpec(isvc, &isvc.ObjectMeta)
		} else {
			actual, err = decoder.reconcilePodSpec(isvc, &isvc.ObjectMeta)
		}
	}
	require.NoError(t, err)
	return actual
}
