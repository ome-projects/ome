package basemodel

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/backends/pernode"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func TestModelDeletionAfterNodeDisappears(t *testing.T) {
	for _, clusterScoped := range []bool{false, true} {
		for _, scenario := range []struct {
			name             string
			keepNode         bool
			deliverNodeEvent bool
			holdConfigMap    bool
		}{
			{name: "missed node deletion event"},
			{name: "delivered node deletion event", deliverNodeEvent: true},
			{name: "existing NotReady node", keepNode: true},
			{name: "orphan ConfigMap has another finalizer", holdConfigMap: true},
		} {
			model, finalizer := orphanedNodeModel(clusterScoped)
			t.Run(model.GetObjectKind().GroupVersionKind().Kind+"/"+scenario.name, func(t *testing.T) {
				g := gomega.NewGomegaWithT(t)
				ctx := context.Background()
				const foreignFinalizer = "other-controller.example.com/cleanup"
				model.SetFinalizers([]string{finalizer, foreignFinalizer})
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "worker"},
					Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}},
				}
				cm := orphanedNodeConfigMap("worker", model, clusterScoped)
				if scenario.holdConfigMap {
					cm.Finalizers = []string{foreignFinalizer}
				}
				unlabeled := orphanedNodeConfigMap("unrelated", model, clusterScoped)
				unlabeled.Labels = nil
				otherNamespace := orphanedNodeConfigMap("worker", model, clusterScoped)
				otherNamespace.Namespace = "other"
				c := orphanedNodeClient(t, model, node, cm, unlabeled, otherNamespace)
				if !scenario.keepNode {
					g.Expect(c.Delete(ctx, node)).To(gomega.Succeed())
					if scenario.deliverNodeEvent {
						pernode.HandleNodeDeletion(ctx, c, ctrl.Log, node)
					}
				}
				g.Expect(c.Delete(ctx, model)).To(gomega.Succeed())
				result, err := reconcileOrphanedNodeModel(ctx, c, model)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(c.Get(ctx, client.ObjectKeyFromObject(model), model)).To(gomega.Succeed())
				g.Expect(model.GetFinalizers()).To(gomega.ContainElement(foreignFinalizer))
				currentCM := &corev1.ConfigMap{}
				getCM := c.Get(ctx, client.ObjectKeyFromObject(cm), currentCM)
				if scenario.keepNode {
					g.Expect(model.GetFinalizers()).To(gomega.ContainElement(finalizer))
					g.Expect(result.RequeueAfter).To(gomega.Equal(30 * time.Second))
					g.Expect(getCM).NotTo(gomega.HaveOccurred())
					g.Expect(currentCM.Data).To(gomega.Equal(cm.Data))
					g.Expect(currentCM.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
				} else {
					g.Expect(model.GetFinalizers()).NotTo(gomega.ContainElement(finalizer))
					g.Expect(result).To(gomega.Equal(ctrl.Result{}))
					if scenario.holdConfigMap {
						g.Expect(getCM).NotTo(gomega.HaveOccurred())
						g.Expect(currentCM.Finalizers).To(gomega.Equal([]string{foreignFinalizer}))
						g.Expect(currentCM.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
					} else {
						g.Expect(errors.IsNotFound(getCM)).To(gomega.BeTrue())
					}
				}
				for _, untouched := range []*corev1.ConfigMap{unlabeled, otherNamespace} {
					actual := &corev1.ConfigMap{}
					g.Expect(c.Get(ctx, client.ObjectKeyFromObject(untouched), actual)).To(gomega.Succeed())
					g.Expect(actual.Data).To(gomega.Equal(untouched.Data))
					g.Expect(actual.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
				}
			})
		}
	}
}

func TestModelStatusCleansOrphanedNodeConfigMap(t *testing.T) {
	for _, clusterScoped := range []bool{false, true} {
		model, finalizer := orphanedNodeModel(clusterScoped)
		t.Run(model.GetObjectKind().GroupVersionKind().Kind, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			ctx := context.Background()
			model.SetFinalizers([]string{finalizer})
			liveNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "live-worker"}}
			liveCM := orphanedNodeConfigMap(liveNode.Name, model, clusterScoped)
			orphanCM := orphanedNodeConfigMap("missing-worker", model, clusterScoped)
			unrelatedCM := orphanedNodeConfigMap("other-model-worker", model, clusterScoped)
			delete(unrelatedCM.Data, constants.GetModelConfigMapKey(model.GetNamespace(), model.GetName(), clusterScoped))
			c := orphanedNodeClient(t, model, liveNode, liveCM, orphanCM, unrelatedCM)

			_, err := reconcileOrphanedNodeModel(ctx, c, model)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(c.Get(ctx, client.ObjectKeyFromObject(model), model)).To(gomega.Succeed())
			_, status, err := shared.ModelSpecAndStatus(model)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
			g.Expect(status.NodesReady).To(gomega.Equal([]string{liveNode.Name}))
			g.Expect(status.NodesFailed).To(gomega.BeEmpty())
			g.Expect(c.Get(ctx, client.ObjectKeyFromObject(liveCM), &corev1.ConfigMap{})).To(gomega.Succeed())
			g.Expect(errors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(orphanCM), &corev1.ConfigMap{}))).To(gomega.BeTrue())
			actual := &corev1.ConfigMap{}
			g.Expect(c.Get(ctx, client.ObjectKeyFromObject(unrelatedCM), actual)).To(gomega.Succeed())
			g.Expect(actual.Data).To(gomega.Equal(unrelatedCM.Data))
		})
	}
}

func orphanedNodeModel(clusterScoped bool) (client.Object, string) {
	spec := v1beta1.BaseModelSpec{
		ModelFormat: v1beta1.ModelFormat{Name: "safetensors"},
		Storage:     &v1beta1.StorageSpec{StorageUri: stringPtr("hf://test/model")},
	}
	meta := metav1.ObjectMeta{Name: "model", UID: types.UID("model-uid")}
	if clusterScoped {
		return &v1beta1.ClusterBaseModel{
			TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "ClusterBaseModel"},
			ObjectMeta: meta, Spec: spec,
		}, constants.ClusterBaseModelFinalizer
	}
	meta.Namespace = "default"
	return &v1beta1.BaseModel{
		TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "BaseModel"},
		ObjectMeta: meta, Spec: spec,
	}, constants.BaseModelFinalizer
}

func orphanedNodeConfigMap(nodeName string, model client.Object, clusterScoped bool) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName, Namespace: constants.OMENamespace, UID: types.UID(nodeName + "-uid"),
			Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"},
		},
		Data: map[string]string{
			constants.GetModelConfigMapKey(model.GetNamespace(), model.GetName(), clusterScoped): `{"status":"Ready"}`,
			"clusterbasemodel.other-model": `{"status":"Ready"}`,
		},
	}
}

func orphanedNodeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	g := gomega.NewGomegaWithT(t)
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())
	g.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
	g.Expect(batchv1.AddToScheme(scheme)).To(gomega.Succeed())
	return ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).
		WithStatusSubresource(&v1beta1.BaseModel{}, &v1beta1.ClusterBaseModel{}).Build()
}

func reconcileOrphanedNodeModel(ctx context.Context, c client.Client, model client.Object) (ctrl.Result, error) {
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(model)}
	if _, clusterScoped := model.(*v1beta1.ClusterBaseModel); clusterScoped {
		r := &ClusterBaseModelReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
		return r.Reconcile(ctx, request)
	}
	r := &BaseModelReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}
	return r.Reconcile(ctx, request)
}
