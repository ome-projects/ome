package pernode

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/modelagent"
)

func TestReconcileEvictionStatus(t *testing.T) {
	tests := []struct {
		name    string
		reports []string
		want    v1beta1.LifeCycleState
	}{
		{"all evicted", []string{"Evicted", "Evicted"}, "Evicted"},
		{"ready wins", []string{"Evicted", "Failed", "Updating", "Ready"}, v1beta1.LifeCycleStateReady},
		{"updating before evicted", []string{"Evicted", "Updating"}, v1beta1.LifeCycleStateInTransit},
		{"updating before failed", []string{"Failed", "Updating"}, v1beta1.LifeCycleStateInTransit},
		{"evicted before failed", []string{"Evicted", "Failed"}, "Evicted"},
		{"only failed", []string{"Failed"}, v1beta1.LifeCycleStateFailed},
		{"no reports", nil, v1beta1.LifeCycleStateInTransit},
		{"deleted is not evicted", []string{"Deleted"}, v1beta1.LifeCycleStateInTransit},
		{"unknown is not evicted", []string{"unknown"}, v1beta1.LifeCycleStateInTransit},
	}
	for _, clusterScoped := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("cluster=%t/%s", clusterScoped, tt.name), func(t *testing.T) {
				ctx := context.Background()
				var obj client.Object = &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default"}}
				if clusterScoped {
					obj = &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model"}}
				}
				_, status, _ := shared.ModelSpecAndStatus(obj)
				status.State = v1beta1.LifeCycleStateReady
				status.NodesReady = []string{"stale"}
				status.NodesFailed = []string{"stale"}
				objects := []client.Object{obj}
				var wantReady, wantFailed []string
				key := constants.GetModelConfigMapKey(obj.GetNamespace(), obj.GetName(), clusterScoped)
				for i, report := range tt.reports {
					name := fmt.Sprintf("node-%d", i)
					raw, err := json.Marshal(modelagent.ModelEntry{Status: modelagent.ModelStatus(report)})
					if err != nil {
						t.Fatal(err)
					}
					objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}, &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: constants.OMENamespace, Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"}},
						Data:       map[string]string{key: string(raw)},
					})
					if report == "Ready" {
						wantReady = append(wantReady, name)
					}
					if report == "Failed" {
						wantFailed = append(wantFailed, name)
					}
				}
				c := newEvictionTestClient(t, objects...)
				if err := ReconcileStatusFromConfigMaps(ctx, c, c, logr.Discard(), obj, clusterScoped, "Model"); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
					t.Fatal(err)
				}
				_, got, _ := shared.ModelSpecAndStatus(obj)
				if got.State != tt.want {
					t.Errorf("state = %s, want %s", got.State, tt.want)
				}
				if !reflect.DeepEqual(got.NodesReady, wantReady) || !reflect.DeepEqual(got.NodesFailed, wantFailed) {
					t.Errorf("node reports = ready %v / failed %v; want %v / %v", got.NodesReady, got.NodesFailed, wantReady, wantFailed)
				}
				// Repeated reconciliation must retain the same terminal observation.
				if err := ReconcileStatusFromConfigMaps(ctx, c, c, logr.Discard(), obj, clusterScoped, "Model"); err != nil {
					t.Fatal(err)
				}
				_, repeated, _ := shared.ModelSpecAndStatus(obj)
				if repeated.State != tt.want {
					t.Errorf("repeated state = %s, want %s", repeated.State, tt.want)
				}
			})
		}
	}
}

func newEvictionTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(&v1beta1.BaseModel{}, &v1beta1.ClusterBaseModel{}).Build()
}
