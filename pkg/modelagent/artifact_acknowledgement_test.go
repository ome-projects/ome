package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeinformers "sigs.k8s.io/ome/pkg/client/informers/externalversions"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/xet"
)

func newAcknowledgementGopher(t *testing.T) (*Gopher, *GopherTask) {
	t.Helper()
	g, task, _ := newDirectArtifactTestModel(t)
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request-1"}
	task.TaskType = DownloadOverride
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.UID = "startup-node"
	_, err = g.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	require.NoError(t, err)
	g, err = NewGopher(g.modelConfigParser, g.configMapReconciler, &xet.Config{}, g.kubeClient,
		1, 1, 1, g.modelRootDir, make(chan *GopherTask, 8), 0, g.nodeLabelReconciler, g.metrics, g.logger, nil, nil, g.modelClient)
	require.NoError(t, err)
	return g, task
}

func TestArtifactAcknowledgementPrecedesReadyLabels(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	client := g.kubeClient.(*k8sfake.Clientset)
	// Force publication even though the fixture's old ordinary label is Ready.
	node, err := client.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)] = string(Updating)
	_, err = client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	require.NoError(t, err)
	patches := 0
	client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		patches++
		object, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), g.configMapReconciler.namespace, g.configMapReconciler.nodeName)
		require.NoError(t, getErr)
		cm := object.(*corev1.ConfigMap)
		var entry map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(cm.Data[getModelID(task.BaseModel, nil)]), &entry))
		require.Equal(t, "request-1", entry["artifactRehydrationID"], "durable acknowledgement must precede Ready")
		require.Equal(t, "startup-node", entry["nodeUID"])
		require.Equal(t, string(task.BaseModel.UID), entry["modelUID"])
		require.Equal(t, "Ready", entry["status"])
		return false, nil, nil
	})
	require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	require.Equal(t, 1, patches)
}

func TestArtifactAcknowledgementFailureDoesNotPublishReady(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	client := g.kubeClient.(*k8sfake.Clientset)
	node, err := client.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)] = string(Updating)
	_, err = client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	require.NoError(t, err)
	patches := 0
	client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		patches++
		return false, nil, nil
	})
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("report unavailable")
	})
	require.Error(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	require.Zero(t, patches)
}

func TestArtifactAcknowledgementRequestLabelValidation(t *testing.T) {
	for _, request := range []string{"restore-001", "550e8400-e29b-41d4-a716-446655440000", "invalid/value", strings.Repeat("x", 64)} {
		t.Run(request, func(t *testing.T) {
			g, task := newAcknowledgementGopher(t)
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = request
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			err := g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
			if strings.Contains(request, "/") || len(request) > 63 {
				require.Error(t, err)
				require.Empty(t, directArtifactEntry(t, g, task).ArtifactRehydrationID)
			} else {
				require.NoError(t, err)
				node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, request, node.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)])
			}
		})
	}
}

func TestArtifactAcknowledgementCachePreservesReadOnlyReport(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	ctx := context.Background()
	key := getModelID(task.BaseModel, nil)
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	entry := directArtifactEntry(t, g, task)
	require.Empty(t, entry.DirectArtifactPath)
	require.Empty(t, entry.HfArtifactKey)
	require.NotEmpty(t, g.configMapReconciler.modelCache[key].ModelEntryJSON)
	// Updating and Failed are not successful validation acknowledgements.
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "request-2"
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	for _, status := range []ModelStateOnNode{Updating, Failed} {
		require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: status}))
		require.Equal(t, "request-1", directArtifactEntry(t, g, task).ArtifactRehydrationID)
	}
	require.NoError(t, g.kubeClient.CoreV1().ConfigMaps(g.configMapReconciler.namespace).Delete(ctx, g.configMapReconciler.nodeName, metav1.DeleteOptions{}))
	g.configMapReconciler.restoreModelInConfigMap(key, g.configMapReconciler.modelCache[key])
	restored := directArtifactEntry(t, g, task)
	require.Equal(t, "request-1", restored.ArtifactRehydrationID)
	require.Equal(t, entry.NodeUID, restored.NodeUID)
	require.Equal(t, entry.ModelUID, restored.ModelUID)
}

func TestArtifactAcknowledgementRejectsReplacementNode(t *testing.T) {
	for _, status := range []ModelStateOnNode{Ready, Updating, Failed} {
		t.Run(string(status), func(t *testing.T) {
			g, task := newAcknowledgementGopher(t)
			node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.UID = "replacement-node"
			_, err = g.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.Error(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: status}))
			require.Empty(t, directArtifactEntry(t, g, task).NodeUID)
			after, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, node.Labels, after.Labels)
		})
	}
}

func TestArtifactAcknowledgementSharedSiblingCannotAcknowledgeRequest(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	ctx := context.Background()
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Updating}))
	factory := omeinformers.NewSharedInformerFactory(g.modelClient, 0)
	models := factory.Ome().V1beta1().BaseModels()
	require.NoError(t, models.Informer().GetIndexer().Add(task.BaseModel))
	g.baseModelLister = models.Lister()
	require.NoError(t, g.updateHfArtifactChildLabels(ctx, map[string]ModelStatus{getModelID(task.BaseModel, nil): ModelStatusReady}))
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(Updating), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
	require.Empty(t, node.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)])
	require.Empty(t, directArtifactEntry(t, g, task).ArtifactRehydrationID)
}

func TestArtifactAcknowledgementRejectsStaleCapturedRequest(t *testing.T) {
	for _, change := range []string{"empty to request", "request", "UID", "source", "path", "intent", "placement"} {
		for _, status := range []ModelStateOnNode{Ready, Updating, Failed} {
			t.Run(change+"/"+string(status), func(t *testing.T) {
				g, task := newAcknowledgementGopher(t)
				ctx := context.Background()
				require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
				before := directArtifactEntry(t, g, task)
				if change == "empty to request" {
					task.BaseModel.Annotations = nil
				}
				latest := task.BaseModel.DeepCopy()
				switch change {
				case "empty to request", "request":
					latest.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "newer"}
				case "UID":
					latest.UID = "newer-uid"
				case "source":
					latest.Spec.Storage.StorageUri = stringPtr("hf://new/model")
				case "path":
					latest.Spec.Storage.Path = stringPtr(g.modelRootDir + "/other")
				case "intent":
					latest.Annotations[ArtifactResidencyAnnotation] = string(ModelStatusEvicted)
				case "placement":
					latest.Spec.Storage.NodeSelector = map[string]string{"absent": "true"}
				}
				g.modelClient = omefake.NewSimpleClientset(latest)
				require.Error(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: status}))
				require.Equal(t, before, directArtifactEntry(t, g, task))
				node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
				require.Equal(t, "request-1", node.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)])
			})
		}
	}
}

func TestArtifactAcknowledgementLostNodePatchRetry(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	client := g.kubeClient.(*k8sfake.Clientset)
	failed := false
	client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !failed {
			failed = true
			return true, nil, fmt.Errorf("lost node patch")
		}
		return false, nil, nil
	})
	op := &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}
	require.Error(t, g.safeNodeLabelReconciliation(context.Background(), op))
	entry := directArtifactEntry(t, g, task)
	require.Equal(t, "request-1", entry.ArtifactRehydrationID)
	require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), op))
	require.Equal(t, entry, directArtifactEntry(t, g, task))
	node, err := client.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "request-1", node.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)])
}

func TestArtifactAcknowledgementReplacementAfterValidation(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	client := g.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", g.configMapReconciler.nodeName)
		require.NoError(t, err)
		node := obj.(*corev1.Node)
		node.UID = "replacement"
		node.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)] = "newer"
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
		// Even a node with a coincident resourceVersion cannot pass the UID test.
		return false, nil, nil
	})
	require.Error(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	node, err := client.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "newer", node.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)])
}

func TestArtifactAcknowledgementWithdrawalRejectsReplacementNode(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	ctx := context.Background()
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.UID = "replacement"
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	// Eviction uses the label reconciler directly, outside safe publication.
	require.Error(t, g.nodeLabelReconciler.ReconcileNodeLabels(&NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted}))
	require.Error(t, g.withdrawRestorationReadiness(ctx, task))
	after, err := g.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, node.Labels, after.Labels)
}

func TestArtifactAcknowledgementDoesNotRebindSiblingReports(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	ctx := context.Background()
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.UID = "replacement"
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	other := task.BaseModel.DeepCopy()
	other.Name, other.UID = "sibling", "sibling-uid"
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel, other)
	restarted, err := NewGopher(g.modelConfigParser, g.configMapReconciler, &xet.Config{}, g.kubeClient,
		1, 1, 1, g.modelRootDir, g.gopherChan, 0, NewNodeLabelReconciler(node.Name, g.kubeClient, 1, g.logger), g.metrics, g.logger, nil, nil, g.modelClient)
	require.NoError(t, err)
	require.NoError(t, restarted.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: other, ModelStateOnNode: Ready}))
	require.Equal(t, "startup-node", string(directArtifactEntry(t, restarted, task).NodeUID))
	require.Equal(t, "replacement", string(directArtifactEntry(t, restarted, &GopherTask{BaseModel: other}).NodeUID))
	key := getModelID(task.BaseModel, nil)
	require.NoError(t, g.kubeClient.CoreV1().ConfigMaps(g.configMapReconciler.namespace).Delete(ctx, node.Name, metav1.DeleteOptions{}))
	g.configMapReconciler.restoreModelInConfigMap(key, g.configMapReconciler.modelCache[key])
	require.Equal(t, "startup-node", string(directArtifactEntry(t, restarted, task).NodeUID))
}

func TestArtifactAcknowledgementLocalReaderReadyIsNonOwning(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprint(shared), func(t *testing.T) {
			g, target, input := newSharedEvictionTestModel(t)
			path := input.ChildModelPath
			if !shared {
				path = filepath.Join(g.modelRootDir, "local")
				require.NoError(t, os.MkdirAll(path, 0700))
			}
			peer := sharedReaderPeer(t, g)
			ctx := context.Background()
			node, err := peer.kubeClient.CoreV1().Nodes().Get(ctx, peer.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.UID = "local-node"
			_, err = peer.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			peer, err = NewGopher(peer.modelConfigParser, peer.configMapReconciler, &xet.Config{}, peer.kubeClient,
				1, 1, 1, peer.modelRootDir, peer.gopherChan, 0, peer.nodeLabelReconciler, peer.metrics, peer.logger, peer.baseModelLister, peer.clusterBaseModelLister, peer.modelClient)
			require.NoError(t, err)
			model := target.BaseModel.DeepCopy()
			model.Name, model.UID = "reader", "reader"
			model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore"}
			model.Spec.Storage = &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}
			require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
			task := &GopherTask{TaskType: DownloadOverride, BaseModel: model}
			require.NoError(t, peer.processTask(task))
			entry := directArtifactEntry(t, peer, task)
			require.Equal(t, ModelStatusReady, entry.Status)
			require.Equal(t, "restore", entry.ArtifactRehydrationID)
			require.Equal(t, "local-node", string(entry.NodeUID))
			require.Empty(t, entry.DirectArtifactPath)
			require.Empty(t, entry.HfArtifactKey)
		})
	}
}

func TestArtifactAcknowledgementDeleteCleansBothLabels(t *testing.T) {
	for _, state := range []string{"both", "request only", "replaced UID"} {
		t.Run(state, func(t *testing.T) {
			g, task := newAcknowledgementGopher(t)
			ctx := context.Background()
			require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
			normal := constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)
			request := constants.GetModelArtifactRequestLabel(task.BaseModel.UID)
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			if state == "request only" {
				delete(node.Labels, normal)
			}
			if state == "replaced UID" {
				latest := task.BaseModel.DeepCopy()
				latest.UID = "replacement-model"
				g.modelClient = omefake.NewSimpleClientset(latest)
				node.Labels[constants.GetModelArtifactRequestLabel(latest.UID)] = "newer-request"
			} else {
				require.NoError(t, g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Delete(ctx, task.BaseModel.Name, metav1.DeleteOptions{}))
			}
			_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			for range 2 {
				require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted}))
				after, err := g.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.NotContains(t, after.Labels, request)
				if state == "replaced UID" {
					require.Equal(t, "Ready", after.Labels[normal])
					require.Equal(t, "newer-request", after.Labels[constants.GetModelArtifactRequestLabel("replacement-model")])
				} else {
					require.NotContains(t, after.Labels, normal)
				}
			}
		})
	}
}

func TestArtifactAcknowledgementDeleteRetryPreservesNewerReady(t *testing.T) {
	g, task := newAcknowledgementGopher(t)
	ctx := context.Background()
	client := g.kubeClient.(*k8sfake.Clientset)
	node, err := client.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.ResourceVersion = "1"
	_, err = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	g.nodeLabelReconciler.opRetry = 2
	client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		latest := task.BaseModel.DeepCopy()
		latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "newer"
		require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
		object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", node.Name)
		require.NoError(t, err)
		newer := object.(*corev1.Node)
		newer.ResourceVersion = "2"
		newer.Labels[constants.GetModelArtifactRequestLabel(latest.UID)] = "newer"
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), newer, ""))
		return false, nil, nil
	})
	require.Error(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted}))
	after, err := client.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Ready", after.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
	require.Equal(t, "newer", after.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)])
	require.Equal(t, "request-1", directArtifactEntry(t, g, task).ArtifactRehydrationID, "stale deletion must not remove the report")
}
