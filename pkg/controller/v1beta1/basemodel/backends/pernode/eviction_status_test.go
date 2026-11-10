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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
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
					raw, err := json.Marshal(shared.ModelEntry{Status: shared.ModelStatus(report)})
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
				c := fakeclient.New(t, fakeclient.WithObjects(objects...), fakeclient.WithStatusSubresource(obj))
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

func TestReconcileEvictionStatusIgnoresUnusableReports(t *testing.T) {
	ctx := context.Background()
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default"}}
	key := constants.GetModelConfigMapKey("default", "model", false)
	cm := func(name, data string) *corev1.ConfigMap {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: constants.OMENamespace, UID: types.UID(name), ResourceVersion: "1", Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"}}, Data: map[string]string{key: data}}
	}
	c := fakeclient.New(t, fakeclient.WithObjects(model,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "failed"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "malformed"}},
		cm("failed", `{"status":"Failed"}`),
		cm("malformed", `{"status":"Evicted"`),
		cm("orphan", `{"status":"Evicted"}`)), fakeclient.WithStatusSubresource(model))
	if err := ReconcileStatusFromConfigMaps(ctx, c, c, logr.Discard(), model, false, "BaseModel"); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(model), model); err != nil {
		t.Fatal(err)
	}
	if model.Status.State != v1beta1.LifeCycleStateFailed {
		t.Fatalf("state = %s, want Failed", model.Status.State)
	}
}

func TestReconcileEvictionRestoreTransitions(t *testing.T) {
	ctx := context.Background()
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default"}}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "node", Namespace: constants.OMENamespace, Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"}}, Data: map[string]string{}}
	c := fakeclient.New(t, fakeclient.WithObjects(model, cm, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}), fakeclient.WithStatusSubresource(model))
	for _, step := range []struct {
		report string
		want   v1beta1.LifeCycleState
	}{
		{"Ready", v1beta1.LifeCycleStateReady},
		{"Updating", v1beta1.LifeCycleStateInTransit},
		{"Evicted", v1beta1.LifeCycleStateEvicted},
		{"Updating", v1beta1.LifeCycleStateInTransit},
		{"Ready", v1beta1.LifeCycleStateReady},
	} {
		cm.Data[constants.GetModelConfigMapKey("default", "model", false)] = fmt.Sprintf(`{"status":%q}`, step.report)
		if err := c.Update(ctx, cm); err != nil {
			t.Fatal(err)
		}
		if err := ReconcileStatusFromConfigMaps(ctx, c, c, logr.Discard(), model, false, "BaseModel"); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(model), model); err != nil {
			t.Fatal(err)
		}
		if model.Status.State != step.want {
			t.Fatalf("report %s: state = %s, want %s", step.report, model.Status.State, step.want)
		}
	}
}
