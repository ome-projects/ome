package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestOrdinaryScopeDoesNotReadNodeOrModelCaches(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{"gpu": "a10"}}}
	w := scopeScout(node)
	w.kubeClient = fake.NewSimpleClientset()
	// Nil listers deliberately prove that ordinary task intake does not depend
	// on scope reconciliation, informer availability, or a current model UID.
	for _, kind := range []GopherTaskType{Download, DownloadOverride, Delete} {
		require.True(t, w.CurrentTaskAllowed(&GopherTask{TaskType: kind, ClusterBaseModel: scopeModel()}))
	}
	w.refreshDownloadScopeNode()
	require.Empty(t, w.kubeClient.(*fake.Clientset).Actions())
}

func TestOrdinaryStorageEligibilityPreservesBaseline(t *testing.T) {
	w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{"gpu": "a10"}}})
	for _, test := range []struct {
		name  string
		terms []corev1.NodeSelectorTerm
		want  bool
	}{
		{"empty-required", nil, true},
		{"empty-term", []corev1.NodeSelectorTerm{{}}, true},
		{"missing-not-in", []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "absent", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"x"}}}}}, false},
		{"matching", []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "gpu", Operator: corev1.NodeSelectorOpIn, Values: []string{"a10"}}}}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			storage := &v1beta1.StorageSpec{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: test.terms}}}
			require.Equal(t, test.want, w.shouldDownloadModel(storage))
			w.DownloadScope = DownloadScope{}
			require.Equal(t, test.want, w.shouldDownloadModel(storage))
			w.DownloadScope = DownloadScope{NodeLabel: "test/scoped", PoolLabel: "test/pool"}
		})
	}
}

func TestClassifiedNodeCannotBecomeOrdinaryByLosingMarkers(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}}}
	w := scopeScout(node)
	require.True(t, w.requiresScope())
	bare := node.DeepCopy()
	bare.Labels = map[string]string{"gpu": "a10"}
	w.nodeInfo, w.scopeNode = bare, bare
	model := scopeModel()
	allowed, known := w.scopeAllowsModel(model, model.Spec.Storage)
	require.False(t, allowed)
	require.False(t, known, "missing identity must not authorize cache deletion")
}

func TestClassifiedInitialSnapshotSurvivesFirstNodeRefresh(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{"gpu": "a10", "test/scoped": "true", "test/pool": "a"}}}
	w := scopeScout(node)
	bare := node.DeepCopy()
	bare.Labels = map[string]string{"gpu": "a10"}
	w.kubeClient = fake.NewSimpleClientset(bare)
	require.True(t, w.refreshNodeForAdd())
	allowed, known := w.scopeAllowsModel(scopeModel(), scopeModel().Spec.Storage)
	require.False(t, allowed)
	require.False(t, known)
}

func TestOrdinaryProjectionChangesDoNotEnqueueEitherModelKind(t *testing.T) {
	w := scopeScout(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"gpu": "a10"}}})
	w.ctx = context.Background()
	w.gopherChan = make(chan *GopherTask, 8)
	projected := scopeModel()
	original := projected.DeepCopy()
	original.Annotations = nil
	original.Spec.Storage.NodeAffinity = nil
	for _, pair := range [][2]*v1beta1.ClusterBaseModel{{original, projected}, {projected, original}} {
		w.updateClusterBaseModel(pair[0], pair[1])
		w.updateBaseModel(&v1beta1.BaseModel{ObjectMeta: pair[0].ObjectMeta, Spec: pair[0].Spec},
			&v1beta1.BaseModel{ObjectMeta: pair[1].ObjectMeta, Spec: pair[1].Spec})
	}
	require.Empty(t, w.gopherChan, "projection and restoration must not restart ordinary downloads")
}
