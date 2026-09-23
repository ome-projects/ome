package pernode

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func TestRehydrationRequiresVerifiedCurrentReports(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, scenario := range []string{"current", "old model UID", "old request", "old node UID", "missing report", "corrupt report", "ordinary not ready", "request label missing", "new participant", "evicted"} {
			t.Run(stringScope(cluster)+"/"+scenario, func(t *testing.T) {
				ctx := context.Background()
				scheme := runtime.NewScheme()
				require.NoError(t, corev1.AddToScheme(scheme))
				require.NoError(t, v1beta1.AddToScheme(scheme))
				meta := metav1.ObjectMeta{Name: "model", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "r2"}}
				if !cluster {
					meta.Namespace = "ns"
				}
				var model client.Object = &v1beta1.BaseModel{ObjectMeta: meta}
				if cluster {
					model = &v1beta1.ClusterBaseModel{ObjectMeta: meta}
				}
				_, status, _ := shared.ModelSpecAndStatus(model)
				status.Rehydration = &v1beta1.ModelRehydrationStatus{RequestID: "r1", CompletedRequestID: "r1"}
				label := constants.GetBaseModelLabel(meta.Namespace, meta.Name)
				if cluster {
					label = constants.GetClusterBaseModelLabel(meta.Name)
				}
				readyLabel, err := constants.ArtifactReadyLabelKey(meta.UID)
				require.NoError(t, err)
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", Labels: map[string]string{label: "Ready", readyLabel: "r2"}}}
				entry := map[string]interface{}{"name": "model", "status": "Ready", "modelUID": "model-uid", "artifactRehydrationID": "r2", "config": map[string]string{"modelArchitecture": "LlamaForCausalLM"}}
				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "node", Namespace: constants.OMENamespace, Annotations: map[string]string{constants.ModelArtifactNodeUIDAnnotation: "node-uid"}, Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"}}}
				switch scenario {
				case "old model UID":
					entry["modelUID"] = "old"
				case "old request":
					entry["artifactRehydrationID"] = "r1"
				case "old node UID":
					cm.Annotations[constants.ModelArtifactNodeUIDAnnotation] = "old"
				case "ordinary not ready":
					node.Labels[label] = "Updating"
				case "request label missing":
					delete(node.Labels, readyLabel)
				case "evicted":
					model.GetAnnotations()[constants.ModelArtifactResidencyAnnotation] = "Evicted"
					entry["status"] = "Evicted"
					node.Labels[label] = "Evicted"
				}
				data, err := json.Marshal(entry)
				require.NoError(t, err)
				cm.Data = map[string]string{constants.GetModelConfigMapKey(meta.Namespace, meta.Name, cluster): string(data)}
				if scenario == "corrupt report" {
					cm.Data[constants.GetModelConfigMapKey(meta.Namespace, meta.Name, cluster)] = "{"
				}
				objects := []client.Object{model, node}
				if scenario != "missing report" {
					objects = append(objects, cm)
				}
				if scenario == "new participant" {
					objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "joining", UID: "joining-uid", Labels: map[string]string{label: "Updating"}}})
				}
				c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(model).WithObjects(objects...).Build()
				require.NoError(t, ReconcileStatusFromConfigMaps(ctx, c, c, logr.Discard(), model, cluster, "Model"))
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(model), model))
				_, status, _ = shared.ModelSpecAndStatus(model)
				require.Equal(t, "r2", model.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation])
				require.Equal(t, "r2", status.Rehydration.RequestID)
				if scenario == "current" {
					require.Equal(t, "r2", status.Rehydration.CompletedRequestID)
				} else {
					require.Equal(t, "r1", status.Rehydration.CompletedRequestID)
				}
				if scenario == "current" || scenario == "new participant" {
					require.Equal(t, []string{"node"}, status.NodesReady)
				} else {
					require.Empty(t, status.NodesReady)
				}
				spec, _, _ := shared.ModelSpecAndStatus(model)
				if scenario == "current" || scenario == "new participant" {
					require.NotNil(t, spec.ModelArchitecture, "retained requests must still propagate verified agent metadata")
					require.Equal(t, "LlamaForCausalLM", *spec.ModelArchitecture)
				} else {
					require.Nil(t, spec.ModelArchitecture, "unverified report metadata must not modify the model")
				}
				if scenario == "evicted" {
					require.Equal(t, "Evicted", model.GetAnnotations()[constants.ModelArtifactResidencyAnnotation])
					require.Equal(t, v1beta1.LifeCycleStateEvicted, status.State)
				}
			})
		}
	}
}

func stringScope(cluster bool) string {
	if cluster {
		return "cluster"
	}
	return "namespaced"
}
