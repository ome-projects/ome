package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	listers "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

type countingBaseModelLister struct {
	listers.BaseModelLister
	calls int
	err   error
}

func (l *countingBaseModelLister) List(selector labels.Selector) ([]*v1beta1.BaseModel, error) {
	l.calls++
	if l.err != nil {
		return nil, l.err
	}
	return l.BaseModelLister.List(selector)
}

func TestDownloadScopeHeartbeatSkipsCatalogReplayButRetriesFailure(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", UID: "uid", Labels: map[string]string{"gpu": "a10", "test/scoped": "true"}}}
	w := scopeScout(node)
	w.kubeClient = fake.NewSimpleClientset(node)
	w.gopherChan = make(chan *GopherTask, 8)
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	lister := &countingBaseModelLister{BaseModelLister: listers.NewBaseModelLister(index), err: fmt.Errorf("temporary cache failure")}
	w.baseModelLister = lister
	w.clusterBaseModelLister = listers.NewClusterBaseModelLister(index)
	w.refreshDownloadScopeNode()
	require.Equal(t, 1, lister.calls)
	lister.err = nil
	w.refreshDownloadScopeNode()
	require.Equal(t, 2, lister.calls, "failed replay must not be marked complete")
	for i := 0; i < 100; i++ {
		w.refreshDownloadScopeNode()
	}
	require.Equal(t, 2, lister.calls, "unchanged selection must not scan all models every tick")
	updated := node.DeepCopy()
	updated.Labels["gpu"] = "h100"
	_, err := w.kubeClient.CoreV1().Nodes().Update(context.Background(), updated, metav1.UpdateOptions{})
	require.NoError(t, err)
	w.refreshDownloadScopeNode()
	require.Equal(t, 3, lister.calls, "any baseline selector label change must still replay")
}

func scopeModel() *v1beta1.ClusterBaseModel {
	uri := "oci://bucket@ns/model"
	m := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "m", UID: "uid"}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
		StorageUri: &uri, NodeSelector: map[string]string{"gpu": "a10"},
		NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "test/scoped", Operator: corev1.NodeSelectorOpDoesNotExist}, {Key: "test/pool", Operator: corev1.NodeSelectorOpDoesNotExist}}},
			{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "test/scoped", Operator: corev1.NodeSelectorOpIn, Values: []string{"true"}}, {Key: "test/pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}},
		}}},
	}}}
	proof, _ := json.Marshal(map[string]interface{}{"version": "v1", "uid": m.UID, "applied": m.Spec.Storage.NodeAffinity, "pools": []string{"a"}})
	m.Annotations = map[string]string{downloadScopeAnnotation: string(proof)}
	return m
}

func scopeScout(node *corev1.Node) *Scout {
	return &Scout{ctx: context.Background(), nodeName: "n", nodeInfo: node, scopeNode: node, logger: zap.NewNop().Sugar(), DownloadScope: DownloadScope{NodeLabel: "test/scoped", PoolLabel: "test/pool"}}
}

func TestDownloadScopeEligibility(t *testing.T) {
	model := scopeModel()
	for _, tt := range []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"ordinary", map[string]string{"gpu": "a10"}, true},
		{"selected", map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}, true},
		{"other-pool", map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "b"}, false},
		{"wrong-shape", map[string]string{"gpu": "h100", "test/scoped": "true", "test/pool": "a"}, false},
		{"quarantine", map[string]string{"gpu": "a10", "test/scoped": "true"}, false},
		{"unclassified-pool", map[string]string{"gpu": "a10", "test/pool": "a"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: tt.labels}})
			require.Equal(t, tt.want, w.modelEligible(model, model.Spec.Storage))
		})
	}
	w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}}})
	for _, change := range []func(*v1beta1.ClusterBaseModel){
		func(m *v1beta1.ClusterBaseModel) { m.Annotations = nil },
		func(m *v1beta1.ClusterBaseModel) { m.UID = "recreated" },
		func(m *v1beta1.ClusterBaseModel) { m.Spec.Storage.NodeAffinity = nil },
	} {
		m := model.DeepCopy()
		change(m)
		require.False(t, w.modelEligible(m, m.Spec.Storage))
	}
}

func TestDownloadScopeQueuedDeleteRevalidatesEligibilityAndUID(t *testing.T) {
	model := scopeModel()
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, index.Add(model))
	w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}}})
	w.clusterBaseModelLister = listers.NewClusterBaseModelLister(index)
	task := &GopherTask{TaskType: Delete, ClusterBaseModel: model}
	require.False(t, w.CurrentTaskAllowed(task), "queued old ineligibility must not delete a now-eligible model")
	m := model.DeepCopy()
	m.Spec.Storage.NodeSelector["gpu"] = "h100"
	require.NoError(t, index.Update(m))
	require.True(t, w.CurrentTaskAllowed(task), "same-UID eligibility loss does not require deletionTimestamp")
	require.False(t, w.CurrentTaskAllowed(&GopherTask{TaskType: Download, ClusterBaseModel: model}))
	m.UID = types.UID("replacement")
	require.NoError(t, index.Update(m))
	require.False(t, w.CurrentTaskAllowed(task))
}

func TestDownloadScopeNodePromotionReplaysModelWithoutEndpointWatch(t *testing.T) {
	old := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{"gpu": "a10", "test/scoped": "true"}}}
	next := old.DeepCopy()
	next.Labels["test/pool"] = "a"
	w := scopeScout(old)
	w.kubeClient = fake.NewSimpleClientset(next)
	ch := make(chan *GopherTask, 8)
	w.gopherChan = ch
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, index.Add(scopeModel()))
	w.clusterBaseModelLister = listers.NewClusterBaseModelLister(index)
	w.baseModelLister = listers.NewBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}))
	w.refreshDownloadScopeNode()
	require.Len(t, ch, 1)
	require.Equal(t, Download, (<-ch).TaskType)
	w.refreshDownloadScopeNode()
	require.Empty(t, ch, "unchanged node must not restart downloads")
}

func TestDownloadScopeProjectionUpdateDoesNotRestartOrdinaryDownloads(t *testing.T) {
	w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"gpu": "a10"}}})
	ch := make(chan *GopherTask, 8)
	w.gopherChan = ch
	old := scopeModel()
	next := old.DeepCopy()
	next.Annotations[downloadScopeAnnotation] = "updated proof for another pool"
	w.updateClusterBaseModel(old, next)
	require.Empty(t, ch)
	bm := &v1beta1.BaseModel{ObjectMeta: old.ObjectMeta, Spec: old.Spec}
	other := bm.DeepCopy()
	other.Annotations = next.Annotations
	w.updateBaseModel(bm, other)
	require.Empty(t, ch)
}

func TestDownloadScopeEmptyRequiredAffinityMatchesNothing(t *testing.T) {
	w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"test/scoped": "true"}}})
	for _, terms := range [][]corev1.NodeSelectorTerm{nil, {{}}} {
		require.False(t, w.shouldDownloadModel(&v1beta1.StorageSpec{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms}}}))
	}
}

func TestDownloadScopeIncompleteProjectionDoesNotDeleteServingCache(t *testing.T) {
	w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}}})
	ch := make(chan *GopherTask, 8)
	w.gopherChan = ch
	old := scopeModel()
	next := old.DeepCopy()
	next.Spec.Storage.NodeAffinity = nil
	w.updateClusterBaseModel(old, next)
	require.Empty(t, ch, "Helm drift is not proof of demand withdrawal")
	require.False(t, w.modelEligible(next, next.Spec.Storage))
	require.False(t, w.mayRemoveIneligibleModel(next, next.Spec.Storage))
}

func TestDownloadScopeConfiguration(t *testing.T) {
	require.NoError(t, (DownloadScope{}).Validate())
	require.NoError(t, (DownloadScope{NodeLabel: "test/scoped", PoolLabel: "test/pool"}).Validate())
	require.Error(t, (DownloadScope{NodeLabel: "test/scoped"}).Validate())
	require.Error(t, (DownloadScope{NodeLabel: "same", PoolLabel: "same"}).Validate())
}

func TestDownloadScopeRefreshAndInformerUpdatesAreRaceSafe(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}}}
	w := scopeScout(node)
	w.kubeClient = fake.NewSimpleClientset(node)
	w.gopherChan = make(chan *GopherTask, 256)
	model := scopeModel()
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, index.Add(model))
	w.clusterBaseModelLister = listers.NewClusterBaseModelLister(index)
	w.baseModelLister = listers.NewBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}))
	var work sync.WaitGroup
	for _, operation := range []func(){
		func() { w.refreshDownloadScopeNode() },
		func() { w.updateClusterBaseModel(model, model.DeepCopy()) },
		func() { w.CurrentTaskAllowed(&GopherTask{TaskType: Download, ClusterBaseModel: model}) },
	} {
		work.Add(1)
		go func(run func()) {
			defer work.Done()
			for i := 0; i < 20; i++ {
				run()
			}
		}(operation)
	}
	work.Wait()
}

func TestDownloadScopeEligibilityLossCarriesCleanupReason(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		name := "BaseModel"
		if cluster {
			name = "ClusterBaseModel"
		}
		t.Run(name, func(t *testing.T) {
			w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}}})
			tasks := make(chan *GopherTask, 2)
			w.gopherChan = tasks
			old := scopeModel()
			next := old.DeepCopy()
			next.Spec.Storage.NodeSelector["gpu"] = "h100"
			if cluster {
				w.updateClusterBaseModel(old, next)
			} else {
				w.updateBaseModel(&v1beta1.BaseModel{ObjectMeta: old.ObjectMeta, Spec: old.Spec},
					&v1beta1.BaseModel{ObjectMeta: next.ObjectMeta, Spec: next.Spec})
			}
			require.Len(t, tasks, 1)
			task := <-tasks
			require.Equal(t, Delete, task.TaskType)
			require.True(t, task.NodeIneligible)
			require.True(t, (&Gopher{}).isModelRemovalConfirmed(task),
				"same-UID eligibility loss must survive the agent's cleanup proof")
		})
	}
}
