package pernode

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestEvictedNodeStatusAggregation(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, tc := range []struct {
			name  string
			other shared.ModelStatus
			want  v1beta1.LifeCycleState
		}{
			{"evicted", shared.ModelStatusEvicted, v1beta1.LifeCycleStateEvicted},
			{"ready sibling", shared.ModelStatusReady, v1beta1.LifeCycleStateReady},
			{"failed sibling", shared.ModelStatusFailed, v1beta1.LifeCycleStateFailed},
			{"updating sibling", shared.ModelStatusUpdating, v1beta1.LifeCycleStateInTransit},
		} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, tc.name), func(t *testing.T) {
				scheme := runtime.NewScheme()
				require.NoError(t, corev1.AddToScheme(scheme))
				require.NoError(t, v1beta1.AddToScheme(scheme))
				meta := metav1.ObjectMeta{Name: "model", UID: "model-uid"}
				var model client.Object
				if cluster {
					model = &v1beta1.ClusterBaseModel{ObjectMeta: meta}
				} else {
					meta.Namespace = "default"
					model = &v1beta1.BaseModel{ObjectMeta: meta}
				}
				objects := []client.Object{model}
				for i, status := range []shared.ModelStatus{shared.ModelStatusEvicted, tc.other} {
					name := fmt.Sprintf("node-%d", i)
					data, err := json.Marshal(shared.ModelEntry{Name: "model", Status: status})
					require.NoError(t, err)
					objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}, &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: constants.OMENamespace, Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"}},
						Data:       map[string]string{constants.GetModelConfigMapKey(meta.Namespace, meta.Name, cluster): string(data)},
					})
				}
				c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(model).WithObjects(objects...).Build()
				require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, c, logr.Discard(), model, cluster, "Model"))
				require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
				_, status, err := shared.ModelSpecAndStatus(model)
				require.NoError(t, err)
				require.Equal(t, tc.want, status.State)
				require.Contains(t, status.NodesEvicted, "node-0")
				require.NotContains(t, status.NodesFailed, "node-0")
			})
		}
	}
}
