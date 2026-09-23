package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestRehydrationReadyPersistsBeforeNodeLabel(t *testing.T) {
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-2"}}}
	model.Spec.Storage = &v1beta1.StorageSpec{Path: stringPtr(t.TempDir()), StorageUri: stringPtr("oci://n/ns/b/models/o/model")}
	key := getModelID(model, nil)
	label := constants.GetBaseModelLabel(model.Namespace, model.Name)
	g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{key: modelEntryJSON(ModelStatusUpdating)}), map[string]string{label: "Updating"})
	g.modelClient = omefake.NewSimpleClientset(model)
	g.kubeClient = g.configMapReconciler.kubeClient
	client := g.kubeClient.(*k8sfake.Clientset)
	node, err := client.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	require.NoError(t, err)
	node.UID = "node-uid"
	_, err = client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.configMapReconciler.InitializeNodeUID(node.UID))
	require.NoError(t, g.nodeLabelReconciler.InitializeNodeUID(node.UID))
	writeAttempted := false
	client.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		writeAttempted = true
		return true, nil, fmt.Errorf("persist unavailable")
	})
	err = g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready})
	require.Error(t, err)
	require.True(t, writeAttempted)
	node, err = client.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Updating", node.Labels[label], "no Ready publication before durable acknowledgement")
}

func TestRehydrationPublishesCurrentAcknowledgement(t *testing.T) {
	ctx := context.Background()
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-2"}}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(t.TempDir()), StorageUri: stringPtr("oci://n/ns/b/models/o/model")}}}
	key := getModelID(model, nil)
	g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{key: modelEntryJSON(ModelStatusUpdating)}), nil)
	g.modelClient = omefake.NewSimpleClientset(model)
	g.kubeClient = g.configMapReconciler.kubeClient
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	require.NoError(t, err)
	node.UID = "node-uid"
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.configMapReconciler.InitializeNodeUID(node.UID))
	require.NoError(t, g.nodeLabelReconciler.InitializeNodeUID(node.UID))
	require.NoError(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready}))
	cm, err := g.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	node, err = g.kubeClient.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	require.NoError(t, err)
	require.True(t, modelRehydrationAcknowledged(&GopherTask{BaseModel: model}, node, cm))
}

func TestRehydrationRejectsReadyAfterSharedEviction(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	handler := g.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-1"
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	// Another process has unlinked this child and cleared its relationship,
	// but has not yet published Evicted. Updating alone is not ownership.
	require.NoError(t, os.Remove(input.ChildModelPath))
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	raw, err := json.Marshal(ModelEntry{Name: task.BaseModel.Name, ModelUID: task.BaseModel.UID, Status: ModelStatusUpdating})
	require.NoError(t, err)
	cm.Data[input.ChildModelKey] = string(raw)
	_, err = g.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	err = g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
	require.Error(t, err)
	cm, err = g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.True(t, hasModelEntryStatus(cm.Data[input.ChildModelKey], ModelStatusUpdating))
}

func TestSharedEvictionCompletionCannotOverwriteRestore(t *testing.T) {
	g, task, path := newArtifactEvictionTestModel(t)
	current := task.BaseModel.DeepCopy()
	delete(current.Annotations, constants.ModelArtifactResidencyAnnotation)
	current.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	_, err := g.modelClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.completeSharedArtifactEviction(context.Background(), task, path))
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.True(t, hasModelEntryStatus(cm.Data[getModelID(current, nil)], ModelStatusReady))
	require.DirExists(t, path)
}

func TestRehydrationOldFailureCannotWithdrawCurrentReady(t *testing.T) {
	old := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-1"}}}
	current := old.DeepCopy()
	current.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	label := constants.GetBaseModelLabel(old.Namespace, old.Name)
	key := getModelID(old, nil)
	g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{key: modelEntryJSON(ModelStatusReady)}), map[string]string{label: "Ready"})
	g.modelClient = omefake.NewSimpleClientset(current)
	err := g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: old, ModelStateOnNode: Failed})
	require.ErrorContains(t, err, "request changed")
	node, err := g.configMapReconciler.kubeClient.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Ready", node.Labels[label])
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.True(t, hasModelEntryStatus(cm.Data[key], ModelStatusReady))
}

func TestRehydrationReplayRequiresCurrentAcknowledgement(t *testing.T) {
	for _, missing := range []string{"none", "request", "modelUID", "nodeUID", "label", "status", "entry"} {
		t.Run(missing, func(t *testing.T) {
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-2"}}}
			task := &GopherTask{BaseModel: model}
			requestLabel, err := constants.ArtifactReadyLabelKey(model.UID)
			require.NoError(t, err)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Labels: map[string]string{constants.GetBaseModelLabel(model.Namespace, model.Name): "Ready", requestLabel: "restore-2"}}}
			entry := map[string]string{"name": "model", "modelUID": "model-uid", "status": "Ready", "artifactRehydrationID": "restore-2"}
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.ModelArtifactNodeUIDAnnotation: "node-uid"}}, Data: map[string]string{}}
			switch missing {
			case "request":
				entry["artifactRehydrationID"] = "restore-1"
			case "modelUID":
				entry["modelUID"] = "previous-uid"
			case "nodeUID":
				cm.Annotations[constants.ModelArtifactNodeUIDAnnotation] = "previous-node-uid"
			case "label":
				delete(node.Labels, requestLabel)
			case "status":
				entry["status"] = "Updating"
			}
			raw, err := json.Marshal(entry)
			require.NoError(t, err)
			if missing != "entry" {
				cm.Data[getModelID(model, nil)] = string(raw)
			}
			require.Equal(t, missing == "none", modelRehydrationAcknowledged(task, node, cm))
		})
	}
}

func TestRehydrationReplayRejectsUnownedPath(t *testing.T) {
	for _, storage := range []*v1beta1.StorageSpec{nil, {}, {Path: stringPtr("")}, {Path: stringPtr("/models/model")}} {
		g := &Gopher{}
		ready, err := g.artifactRequestAlreadyReady(context.Background(), &GopherTask{BaseModel: &v1beta1.BaseModel{Spec: v1beta1.BaseModelSpec{Storage: storage}}})
		require.False(t, ready)
		require.ErrorContains(t, err, "owned path")
	}
}

func TestRehydrationRequestQueuesDownload(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(map[bool]string{false: "BaseModel", true: "ClusterBaseModel"}[cluster], func(t *testing.T) {
			old := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-1"}}}
			old.Spec.Storage = &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")}
			current := old.DeepCopy()
			current.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
			tasks := make(chan *GopherTask, 2)
			scout := &Scout{gopherChan: tasks, logger: zap.NewNop().Sugar()}
			if cluster {
				scout.updateClusterBaseModel(&v1beta1.ClusterBaseModel{ObjectMeta: old.ObjectMeta, Spec: old.Spec}, &v1beta1.ClusterBaseModel{ObjectMeta: current.ObjectMeta, Spec: current.Spec})
			} else {
				scout.updateBaseModel(old, current)
			}
			require.Len(t, tasks, 1)
			require.Equal(t, Download, (<-tasks).TaskType)
		})
	}
}

func TestRehydrationRejectsStaleTask(t *testing.T) {
	for _, change := range []string{"request", "uid", "source", "path"} {
		t.Run(change, func(t *testing.T) {
			old := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-1"}}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/models/o/source"), Path: stringPtr("/models/model")}}}
			current := old.DeepCopy()
			switch change {
			case "request":
				current.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
			case "uid":
				current.UID = "new-uid"
			case "source":
				current.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/models/o/other")
			case "path":
				current.Spec.Storage.Path = stringPtr("/models/other")
			}
			index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			require.NoError(t, index.Add(current))
			g := &Gopher{baseModelLister: modelslister.NewBaseModelLister(index), modelClient: omefake.NewSimpleClientset(current), logger: zap.NewNop().Sugar()}
			skip, cleanup, err := g.shouldSkipStaleDownloadTask(context.Background(), &GopherTask{TaskType: Download, BaseModel: old, ResidencyManaged: true})
			require.NoError(t, err)
			require.True(t, skip)
			require.False(t, cleanup)
		})
	}
}

func TestRehydrationRejectsMissingStorage(t *testing.T) {
	for _, storage := range []*v1beta1.StorageSpec{nil, {}, {Path: stringPtr("/models/model")}, {StorageUri: stringPtr("oci://n/ns/b/models/o/model"), Path: stringPtr("")}} {
		model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-1"}}, Spec: v1beta1.BaseModelSpec{Storage: storage}}
		g := &Gopher{modelClient: omefake.NewSimpleClientset(model)}
		_, _, err := g.shouldSkipArtifactTask(context.Background(), &GopherTask{TaskType: Download, BaseModel: model})
		require.ErrorContains(t, err, "explicit storage URI and path")
	}
}

func TestNewRehydrationRequestValidatesReadyParent(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	handler := g.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	// This process already validated its startup parents. A new request must
	// still confirm the bytes it is about to acknowledge.
	g.hfArtifactStartup.recovered = true
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	validated, downloaded := 0, 0
	result, err := g.runHfArtifactDownload(context.Background(), task, input, true,
		func(string) (bool, error) { validated++; return true, nil },
		func(string) error { downloaded++; return nil })
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	require.Equal(t, 1, validated)
	require.Zero(t, downloaded)
}

func TestDirectHfRehydrationValidatesExistingCopy(t *testing.T) {
	g, task, input, source, downloads := newTestDirectHfSource(t)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	require.NoError(t, os.MkdirAll(input.ChildModelPath, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "config.json"), []byte("{}"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "model.safetensors"), []byte("weights"), 0600))
	waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.False(t, waiting)
	require.Zero(t, *downloads, "healthy direct copy needs validation, not replacement")
	info, err := os.Lstat(input.ChildModelPath)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestDirectHfRehydrationRepairsSameSizeCorruption(t *testing.T) {
	g, task, input, source, downloads := newTestDirectHfSource(t)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	g.kubeClient = g.configMapReconciler.kubeClient
	require.NoError(t, os.MkdirAll(input.ChildModelPath, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "config.json"), []byte("{}"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "model.safetensors"), []byte("damaged"), 0600))
	waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.False(t, waiting)
	require.Equal(t, 1, *downloads)
	contents, err := os.ReadFile(filepath.Join(input.ChildModelPath, "model.safetensors"))
	require.NoError(t, err)
	require.Equal(t, "weights", string(contents))
}

func TestRehydrationPeriodicReplay(t *testing.T) {
	ctx := context.Background()
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-2"}}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/models/o/source"), Path: stringPtr("/models/model")}}}
	modelIndex := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, modelIndex.Add(model))
	clusterIndex := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid"}}
	client := k8sfake.NewSimpleClientset(node)
	tasks := make(chan *GopherTask, 2)
	scout := &Scout{nodeName: node.Name, configMapNamespace: "ome", logger: zap.NewNop().Sugar(), kubeClient: client, baseModelLister: modelslister.NewBaseModelLister(modelIndex), clusterBaseModelLister: modelslister.NewClusterBaseModelLister(clusterIndex), gopherChan: tasks}
	require.NoError(t, scout.reconcileArtifactRequests(ctx))
	task := <-tasks
	require.Equal(t, Download, task.TaskType)
	key, err := constants.ArtifactReadyLabelKey(model.UID)
	require.NoError(t, err)
	node.Labels = map[string]string{key: "restore-2", constants.GetBaseModelLabel(model.Namespace, model.Name): "Ready"}
	_, err = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	raw, err := json.Marshal(ModelEntry{Name: model.Name, ModelUID: model.UID, Status: ModelStatusReady, ArtifactRehydrationID: "restore-2"})
	require.NoError(t, err)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: node.Name, Namespace: "ome", Annotations: map[string]string{constants.ModelArtifactNodeUIDAnnotation: string(node.UID)}}, Data: map[string]string{getModelID(model, nil): string(raw)}}
	_, err = client.CoreV1().ConfigMaps("ome").Create(ctx, cm, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, scout.reconcileArtifactRequests(ctx))
	require.Empty(t, tasks, "completed durable requests are not replayed")
}
