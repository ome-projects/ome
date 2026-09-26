package modelagent

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeinformers "sigs.k8s.io/ome/pkg/client/informers/externalversions"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestScoutPeriodicallyRecoversArtifactAcknowledgements(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	g, task := newAcknowledgementGopher(t)
	ctx := context.Background()
	task.BaseModel.Spec.ModelFormat.Name = constants.TensorRTLLM
	task.BaseModel.Spec.AdditionalMetadata = map[string]string{"type": "draft"}
	task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"eligible": "yes"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.Labels["eligible"] = "yes"
	node.Labels[constants.NodeInstanceShapeLabel] = "GPU.A10.2"
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	factory := omeinformers.NewSharedInformerFactory(g.modelClient, 0)
	models := factory.Ome().V1beta1().BaseModels()
	clusters := factory.Ome().V1beta1().ClusterBaseModels()
	queue := make(chan *GopherTask, 100)
	scout := &Scout{ctx: ctx, kubeClient: g.kubeClient, nodeName: node.Name, configMapNamespace: g.configMapReconciler.namespace,
		baseModelLister: models.Lister(), baseModelSynced: models.Informer().HasSynced,
		clusterBaseModelLister: clusters.Lister(), clusterBaseModelSynced: clusters.Informer().HasSynced,
		informerFactory: factory, gopherChan: queue, logger: g.logger, acknowledgementInterval: 10 * time.Millisecond,
		// Recovery must not use this stale startup placement or TRT shape.
		nodeInfo: &corev1.Node{}, nodeShapeAlias: "stale"}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- scout.Run(stop) }()
	t.Cleanup(func() { close(stop); require.NoError(t, <-done) })
	require.True(t, cache.WaitForCacheSync(stop, models.Informer().HasSynced, clusters.Informer().HasSynced))
	select {
	case <-queue:
		t.Fatal("current acknowledgement needs no validation")
	case <-time.After(30 * time.Millisecond):
	}
	// No informer change: a lost request label alone needs periodic validation.
	node, err = g.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	delete(node.Labels, constants.GetModelArtifactRequestLabel(task.BaseModel.UID))
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	select {
	case recovery := <-queue:
		require.Equal(t, DownloadOverride, recovery.TaskType)
		require.False(t, recovery.RevalidationReplay, "recovery must execute validation, not cheap replay")
		require.Equal(t, task.BaseModel.UID, recovery.BaseModel.UID)
		require.Equal(t, &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ShapeAlias: "GPU.A10.2", ModelType: "draft"}, recovery.TensorRTLLMShapeFilter)
	case <-time.After(time.Second):
		t.Fatal("Scout did not periodically recover a lost Ready acknowledgement label")
	}
}

func TestScoutAcknowledgementEvidence(t *testing.T) {
	for _, fault := range []string{"missing CM", "malformed sibling", "old UID", "old request", "old node", "Updating", "off node", "evicted", "current"} {
		t.Run(fault, func(t *testing.T) {
			g, task := newAcknowledgementGopher(t)
			ctx := context.Background()
			require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
			if fault == "missing CM" {
				require.NoError(t, g.kubeClient.CoreV1().ConfigMaps(g.configMapReconciler.namespace).Delete(ctx, g.configMapReconciler.nodeName, metav1.DeleteOptions{}))
			} else {
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry := directArtifactEntry(t, g, task)
				switch fault {
				case "old UID":
					entry.ModelUID = "old"
				case "old request":
					entry.ArtifactRehydrationID = "old"
				case "old node":
					entry.NodeUID = "old"
				case "Updating":
					entry.Status = ModelStatusUpdating
				case "off node":
					task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"missing": "yes"}
				case "evicted":
					task.BaseModel.Annotations[ArtifactResidencyAnnotation] = string(ModelStatusEvicted)
				}
				raw, err := json.Marshal(entry)
				require.NoError(t, err)
				cm.Data[getModelID(task.BaseModel, nil)] = string(raw)
				if fault == "malformed sibling" {
					cm.Data[getModelID(task.BaseModel, nil)] = "{malformed"
				}
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			factory := omeinformers.NewSharedInformerFactory(omefake.NewSimpleClientset(), 0)
			models := factory.Ome().V1beta1().BaseModels()
			clusters := factory.Ome().V1beta1().ClusterBaseModels()
			require.NoError(t, models.Informer().GetIndexer().Add(task.BaseModel))
			// Cluster models use the same UID/R/node acknowledgement contract.
			cbm := &v1beta1.ClusterBaseModel{ObjectMeta: *task.BaseModel.ObjectMeta.DeepCopy(), Spec: task.BaseModel.Spec}
			cbm.Namespace = ""
			require.NoError(t, clusters.Informer().GetIndexer().Add(cbm))
			queue := make(chan *GopherTask, 4)
			scout := &Scout{ctx: ctx, kubeClient: g.kubeClient, nodeName: g.configMapReconciler.nodeName,
				configMapNamespace: g.configMapReconciler.namespace, baseModelLister: models.Lister(), clusterBaseModelLister: clusters.Lister(), gopherChan: queue, logger: g.logger}
			require.NoError(t, scout.reconcileArtifactAcknowledgements(ctx))
			expected := 2
			if fault == "current" || fault == "malformed sibling" {
				expected = 1
			}
			if fault == "off node" || fault == "evicted" {
				expected = 0
			}
			require.Len(t, queue, expected)
			for len(queue) != 0 {
				require.Equal(t, DownloadOverride, (<-queue).TaskType)
			}
		})
	}
}

func TestArtifactRecoveryPreservesQueuedOverrideValidation(t *testing.T) {
	for _, recoveryFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit first", true: "recovery first"}[recoveryFirst], func(t *testing.T) {
			g, task, _, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			require.Equal(t, 1, *downloads)
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "request"
			explicit := &GopherTask{TaskType: DownloadOverride, BaseModel: task.BaseModel.DeepCopy()}
			recovery := &GopherTask{TaskType: DownloadOverride, BaseModel: task.BaseModel.DeepCopy()}
			ordered := []*GopherTask{explicit, recovery}
			if recoveryFirst {
				ordered = []*GopherTask{recovery, explicit}
			}
			for _, queued := range ordered {
				g.enqueueTask(queued)
			}
			validations := 0
			manifest := source.manifest
			source.manifest = func(ctx context.Context, id, revision, token, endpoint string) (hfSnapshotManifest, error) {
				validations++
				return manifest(ctx, id, revision, token, endpoint)
			}
			for range ordered {
				next, ok := g.taskQueue.popNormal()
				require.True(t, ok)
				require.Equal(t, DownloadOverride, next.TaskType)
				ctx, finish, proceed, err := g.beginTask(next)
				require.NoError(t, err)
				require.True(t, proceed)
				waiting, err := source.process(ctx, g, next, next.BaseModel.Spec, true)
				finish(false)
				require.NoError(t, err)
				require.False(t, waiting)
			}
			require.Equal(t, 2, validations, "recovery must never replace explicit validation with metadata-only replay")
			require.Equal(t, 1, *downloads, "healthy byte validation does not download again")
		})
	}
}
