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

func TestRehydrationEmptyAffinityTerms(t *testing.T) {
	labelTerm := corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{
		Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"},
	}}}
	fieldTerm := corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{
		Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu-node"},
	}}}
	for _, cluster := range []bool{false, true} {
		for _, tc := range []struct {
			name         string
			terms        []corev1.NodeSelectorTerm
			nodeSelector map[string]string
			wantReady    bool
		}{
			{name: "nil terms"},
			{name: "empty terms", terms: []corev1.NodeSelectorTerm{}},
			{name: "empty terms with selector", nodeSelector: map[string]string{"pool": "gpu"}},
			{name: "empty term", terms: []corev1.NodeSelectorTerm{{}}},
			{name: "empty then label term", terms: []corev1.NodeSelectorTerm{{}, labelTerm}, wantReady: true},
			{name: "label then empty term", terms: []corev1.NodeSelectorTerm{labelTerm, {}}, wantReady: true},
			{name: "empty then field term", terms: []corev1.NodeSelectorTerm{{}, fieldTerm}, wantReady: true},
		} {
			t.Run(stringScope(cluster)+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				scheme := runtime.NewScheme()
				require.NoError(t, corev1.AddToScheme(scheme))
				require.NoError(t, v1beta1.AddToScheme(scheme))
				meta := metav1.ObjectMeta{Name: "model", UID: "model-uid", Annotations: map[string]string{
					constants.ModelArtifactRehydrationIDAnnotation: "r2",
				}}
				var model client.Object
				if cluster {
					model = &v1beta1.ClusterBaseModel{ObjectMeta: meta}
				} else {
					meta.Namespace = "ns"
					model = &v1beta1.BaseModel{ObjectMeta: meta}
				}
				spec, status, err := shared.ModelSpecAndStatus(model)
				require.NoError(t, err)
				spec.Storage = &v1beta1.StorageSpec{
					NodeSelector: tc.nodeSelector,
					NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: tc.terms,
					}},
				}
				status.Rehydration = &v1beta1.ModelRehydrationStatus{RequestID: "r1", CompletedRequestID: "r1"}
				label := constants.GetBaseModelLabel(meta.Namespace, meta.Name)
				if cluster {
					label = constants.GetClusterBaseModelLabel(meta.Name)
				}
				readyLabel, err := constants.ArtifactReadyLabelKey(meta.UID)
				require.NoError(t, err)
				gpu := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-node", UID: "gpu-uid", Labels: map[string]string{
					"pool": "gpu", label: "Ready", readyLabel: "r2",
				}}}
				cpu := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "cpu-node", UID: "cpu-uid", Labels: map[string]string{"pool": "cpu"}}}
				data, err := json.Marshal(shared.ModelEntry{
					Name: meta.Name, ModelUID: meta.UID, Status: shared.ModelStatusReady, ArtifactRehydrationID: "r2",
				})
				require.NoError(t, err)
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name: gpu.Name, Namespace: constants.OMENamespace,
						Labels:      map[string]string{constants.ModelStatusConfigMapLabel: "true"},
						Annotations: map[string]string{constants.ModelArtifactNodeUIDAnnotation: string(gpu.UID)},
					},
					Data: map[string]string{constants.GetModelConfigMapKey(meta.Namespace, meta.Name, cluster): string(data)},
				}
				c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(model).WithObjects(model, gpu, cpu, cm).Build()
				require.NoError(t, ReconcileStatusFromConfigMaps(ctx, c, c, logr.Discard(), model, cluster, "Model"))
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(model), model))
				_, status, err = shared.ModelSpecAndStatus(model)
				require.NoError(t, err)
				require.Equal(t, "r2", status.Rehydration.RequestID)
				if tc.wantReady {
					require.Equal(t, []string{gpu.Name}, status.NodesReady)
					require.Equal(t, "r2", status.Rehydration.CompletedRequestID, "an unrelated CPU node must not block completion")
				} else {
					require.Empty(t, status.NodesReady, "empty required affinity must exclude even a labeled, acknowledged node")
					require.Equal(t, "r1", status.Rehydration.CompletedRequestID)
				}
			})
		}
	}
}
