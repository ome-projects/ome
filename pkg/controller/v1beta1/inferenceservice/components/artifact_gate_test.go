package components

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	lwsreconciler "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/lws"
)

func TestArtifactGateRenderedLeaderWorkerTemplates(t *testing.T) {
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		for _, mode := range []string{"restore", "healthy", "legacy"} {
			t.Run(string(component)+"/"+mode, func(t *testing.T) {
				meta := &metav1.ObjectMeta{Name: "model", Namespace: "ns", UID: "model-uid", Annotations: map[string]string{}}
				isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "ns", Annotations: map[string]string{}}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
				if mode != "legacy" {
					isvc.Annotations[constants.ArtifactStartupGateAnnotation] = constants.ArtifactStartupGateAdmitted
					isvc.Annotations[constants.ArtifactModelUIDAnnotation] = string(meta.UID)
				}
				if mode == "restore" {
					meta.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "r2"
					isvc.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "r2"
				}
				key, err := constants.ArtifactReadyLabelKey(meta.UID)
				require.NoError(t, err)
				ordinary := constants.GetBaseModelLabel(meta.Namespace, meta.Name)
				leader := &v1beta1.LeaderSpec{PodSpec: v1beta1.PodSpec{NodeSelector: map[string]string{"pool": "leader", ordinary: "old"}}, Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "model", Image: "model:latest"}}}
				worker := &v1beta1.WorkerSpec{Size: intPtr(1), PodSpec: v1beta1.PodSpec{NodeSelector: map[string]string{"pool": "worker", ordinary: "old"}}, Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "model", Image: "model:latest"}}}
				placement := v1beta1.PodSpec{NodeSelector: map[string]string{ordinary: "old"}}
				if mode == "restore" {
					placement.NodeSelector[key] = "old-request"
				}
				deps := &ComponentDeps{Config: &controllerconfig.InferenceServicesConfig{}}
				in := ComponentInputs{DeploymentMode: constants.MultiNode, BaseModel: &v1beta1.BaseModelSpec{}, BaseModelMeta: meta}
				objectMeta := metav1.ObjectMeta{Name: "endpoint-" + string(component), Namespace: "ns", Labels: map[string]string{}, Annotations: map[string]string{}}
				var leaderPod, workerPod *corev1.PodSpec
				var ext *v1beta1.ComponentExtensionSpec
				if component == v1beta1.EngineComponent {
					spec := &v1beta1.EngineSpec{PodSpec: placement, Leader: leader, Worker: worker}
					isvc.Spec.Engine = spec
					engine := NewEngine(deps, in, spec).(*Engine)
					leaderPod, err = engine.reconcilePodSpec(isvc, &objectMeta)
					require.NoError(t, err)
					workerPod, err = engine.reconcileWorkerPodSpec(isvc, &objectMeta)
					ext = &spec.ComponentExtensionSpec
				} else {
					spec := &v1beta1.DecoderSpec{PodSpec: placement, Leader: leader, Worker: worker}
					isvc.Spec.Decoder = spec
					decoder := NewDecoder(deps, in, spec).(*Decoder)
					leaderPod, err = decoder.reconcilePodSpec(isvc, &objectMeta)
					require.NoError(t, err)
					workerPod, err = decoder.reconcileWorkerPodSpec(isvc, &objectMeta)
					ext = &spec.ComponentExtensionSpec
				}
				require.NoError(t, err)
				require.NotNil(t, workerPod)
				lws := lwsreconciler.NewLWSReconciler(nil, nil, leaderPod, workerPod, 1, ext, objectMeta).LWS
				require.EqualValues(t, 2, *lws.Spec.LeaderWorkerTemplate.Size)
				for role, pod := range map[string]*corev1.PodSpec{"leader": &lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec, "worker": &lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec} {
					require.Equal(t, role, pod.NodeSelector["pool"])
					if mode == "legacy" {
						require.Equal(t, "old", pod.NodeSelector[ordinary], role)
					} else {
						require.Equal(t, "Ready", pod.NodeSelector[ordinary], role)
					}
					if mode == "restore" {
						require.Equal(t, "r2", pod.NodeSelector[key], role)
					} else {
						require.NotContains(t, pod.NodeSelector, key, role)
					}
				}
			})
		}
	}
}
