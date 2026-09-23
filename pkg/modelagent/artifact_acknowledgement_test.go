package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestArtifactAcknowledgementRetainsCompletedRequest(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	require.NoError(t, c.InitializeNodeUID("node-uid"))
	ctx := context.Background()
	_, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "completed"}
	op := &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}
	require.NoError(t, c.ReconcileModelStatus(ctx, op))
	assert.EqualValues(t, "node-uid", op.NodeUID)
	model.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new-request"
	op.ModelStatus = ModelStatusUpdating
	require.NoError(t, c.ReconcileModelStatus(ctx, op))
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(model, nil))
	require.NoError(t, err)
	assert.Equal(t, model.UID, entry.ModelUID)
	assert.Equal(t, "completed", entry.ArtifactRehydrationID)
	assert.Equal(t, "node-uid", cm.Annotations[constants.ModelArtifactNodeUIDAnnotation])
	assert.Equal(t, "completed", c.modelCache[getModelID(model, nil)].ArtifactRehydrationID)
	op.ModelStatus = ModelStatusReady
	require.NoError(t, c.ReconcileModelStatus(ctx, op))
	cm, err = c.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err = existingModelEntry(cm.Data, getModelID(model, nil))
	require.NoError(t, err)
	assert.Equal(t, "new-request", entry.ArtifactRehydrationID)
}

func TestArtifactAcknowledgementReplacesRequestLabel(t *testing.T) {
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "new-request"}
	requestKey, err := constants.ArtifactReadyLabelKey(model.UID)
	require.NoError(t, err)
	modelKey := constants.GetBaseModelLabel(model.Namespace, model.Name)
	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node", UID: "node-uid", ResourceVersion: "1", Labels: map[string]string{modelKey: string(Ready), requestKey: "old-request", "unrelated": "preserved"},
	}})
	n := NewNodeLabelReconciler("node", client, 1, zap.NewNop().Sugar())
	require.NoError(t, n.InitializeNodeUID("node-uid"))
	require.NoError(t, n.applyNodeLabelOperation(&NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready, NodeUID: "node-uid"}))
	node, err := client.CoreV1().Nodes().Get(context.Background(), "node", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "new-request", node.Labels[requestKey])
	assert.Equal(t, "preserved", node.Labels["unrelated"])
	assert.Equal(t, string(Ready), node.Labels[modelKey])
}

func TestArtifactAcknowledgementNodePatchPreconditions(t *testing.T) {
	for _, changedField := range []string{"uid", "resourceVersion"} {
		t.Run(changedField, func(t *testing.T) {
			model := createTestBaseModelCM()
			model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
			original := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1"}}
			client := fake.NewSimpleClientset(original)
			client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				changed := original.DeepCopy()
				if changedField == "uid" {
					changed.UID = "replacement-node"
				} else {
					changed.ResourceVersion = "2"
					changed.Labels = map[string]string{"concurrent": "preserved"}
				}
				require.NoError(t, client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "nodes"}, changed, ""))
				return false, nil, nil
			})
			n := NewNodeLabelReconciler("node", client, 1, zap.NewNop().Sugar())
			require.NoError(t, n.InitializeNodeUID("node-uid"))
			require.Error(t, n.applyNodeLabelOperation(&NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready, NodeUID: original.UID}))
			node, err := client.CoreV1().Nodes().Get(context.Background(), "node", metav1.GetOptions{})
			require.NoError(t, err)
			requestKey, err := constants.ArtifactReadyLabelKey(model.UID)
			require.NoError(t, err)
			assert.NotContains(t, node.Labels, requestKey)
		})
	}
}

func TestArtifactAcknowledgementNodeLabelRejectsInvalidPublication(t *testing.T) {
	for _, scenario := range []string{"node mismatch", "missing node identity", "invalid request", "lookup failure"} {
		t.Run(scenario, func(t *testing.T) {
			model := createTestBaseModelCM()
			model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
			client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1"}})
			op := &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready, NodeUID: "node-uid"}
			switch scenario {
			case "node mismatch":
				op.NodeUID = "old-node"
			case "missing node identity":
				op.NodeUID = ""
			case "invalid request":
				model.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "invalid/request"
			case "lookup failure":
				op.ModelStateOnNode = Updating
				client.PrependReactor("get", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("lookup unavailable")
				})
			}
			n := NewNodeLabelReconciler("node", client, 1, zap.NewNop().Sugar())
			require.NoError(t, n.InitializeNodeUID("node-uid"))
			require.Error(t, n.applyNodeLabelOperation(op))
			for _, action := range client.Actions() {
				assert.NotEqual(t, "patch", action.GetVerb())
			}
		})
	}
}

func TestArtifactAcknowledgementLegacyRegistersOnReady(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	model := createTestBaseModelCM()
	key := getModelID(model, nil)
	_, err := client.CoreV1().ConfigMaps(c.namespace).Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: c.nodeName}, Data: map[string]string{key: `{"name":"test-model","status":"Ready"}`},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusUpdating}))
	before, err := c.getConfigMap(context.Background())
	require.NoError(t, err)
	unknown, err := existingModelEntry(before.Data, key)
	require.NoError(t, err)
	assert.Empty(t, unknown.ModelUID, "a non-Ready transition must not adopt an unknown legacy owner")
	require.NoError(t, c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
	cm, err := c.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, key)
	require.NoError(t, err)
	assert.Equal(t, model.UID, entry.ModelUID)
	assert.Empty(t, entry.ArtifactRehydrationID)
	assert.Empty(t, cm.Annotations[constants.ModelArtifactNodeUIDAnnotation])
}

func TestArtifactAcknowledgementBindingWithdrawalFailureDoesNotCommit(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	require.NoError(t, c.InitializeNodeUID("new-node"))
	ctx := context.Background()
	requestKey, err := constants.ArtifactReadyLabelKey("other-model")
	require.NoError(t, err)
	_, err = client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, UID: "new-node", ResourceVersion: "1", Labels: map[string]string{requestKey: "stale"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("withdrawal unavailable")
	})
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
	op := &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}
	require.Error(t, c.ReconcileModelStatus(ctx, op))
	assert.Empty(t, op.NodeUID)
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "configmaps" {
			assert.NotEqual(t, "create", action.GetVerb())
			assert.NotEqual(t, "update", action.GetVerb())
		}
	}
}

func TestArtifactAcknowledgementRejectsPersistedWrongUID(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	model := createTestBaseModelCM()
	key := getModelID(model, nil)
	_, err := client.CoreV1().ConfigMaps(c.namespace).Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: c.nodeName},
		Data:       map[string]string{key: `{"name":"test-model","modelUID":"old-uid","status":"Ready","artifactRehydrationID":"completed"}`},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "new-request"}
	require.Error(t, c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusUpdating}))
	cm, err := c.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, key)
	require.NoError(t, err)
	assert.EqualValues(t, "old-uid", entry.ModelUID)
	assert.Equal(t, "completed", entry.ArtifactRehydrationID)
}

func TestArtifactAcknowledgementNodeUIDChangeInvalidatesOtherReady(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	require.NoError(t, c.InitializeNodeUID("new-node"))
	ctx := context.Background()
	_, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, UID: "new-node"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().ConfigMaps(c.namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, Annotations: map[string]string{constants.ModelArtifactNodeUIDAnnotation: "old-node"}},
		Data:       map[string]string{"default.basemodel.other": `{"name":"other","modelUID":"other-uid","status":"Ready","artifactRehydrationID":"old"}`},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "new"}
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	assert.Equal(t, "new-node", cm.Annotations[constants.ModelArtifactNodeUIDAnnotation])
	other, err := existingModelEntry(cm.Data, "default.basemodel.other")
	require.NoError(t, err)
	assert.NotEqual(t, ModelStatusReady, other.Status)
	assert.Equal(t, "old", other.ArtifactRehydrationID)
}

func TestArtifactAcknowledgementFirstBindingPreservesOrdinaryReady(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	ctx := context.Background()
	require.NoError(t, c.InitializeNodeUID("node-uid"))
	_, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	ordinary := createTestBaseModelCM()
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: ordinary, ModelStatus: ModelStatusReady}))
	key := getModelID(ordinary, nil)
	before, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	require.Empty(t, before.Annotations[constants.ModelArtifactNodeUIDAnnotation])

	requested := ordinary.DeepCopy()
	requested.Name, requested.UID = "requested", "requested-uid"
	requested.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "first-request"}
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: requested, ModelStatus: ModelStatusReady}))
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	assert.Equal(t, before.Data[key], cm.Data[key], "binding a first request must not alter ordinary model state")
	assert.Equal(t, ModelStatusReady, c.modelCache[key].ModelStatus)

	delete(cm.Data, key)
	_, err = client.CoreV1().ConfigMaps(c.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	c.recreateConfigMap(ctx)
	cm, err = c.getConfigMap(ctx)
	require.NoError(t, err)
	restored, err := existingModelEntry(cm.Data, key)
	require.NoError(t, err)
	assert.Equal(t, ModelStatusReady, restored.Status, "recovery must also retain ordinary readiness")
	assert.Empty(t, restored.ArtifactRehydrationID)
}

func TestArtifactAcknowledgementReadyRequiresNode(t *testing.T) {
	c, _, _ := setupConfigMapTest(t)
	require.NoError(t, c.InitializeNodeUID("node-uid"))
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
	require.Error(t, c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
}

func TestArtifactAcknowledgementNewUpdatingRecordHasOwner(t *testing.T) {
	c, _, _ := setupConfigMapTest(t)
	model := createTestBaseModelCM()
	require.NoError(t, c.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusUpdating}))
	cm, err := c.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(model, nil))
	require.NoError(t, err)
	assert.Equal(t, model.UID, entry.ModelUID)
	assert.Empty(t, entry.ArtifactRehydrationID)
}

func TestArtifactAcknowledgementBindingSkipsForeignData(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	require.NoError(t, c.InitializeNodeUID("node-uid"))
	ctx := context.Background()
	_, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().ConfigMaps(c.namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: c.nodeName}, Data: map[string]string{"agent": `{}`, "foreign": "not model JSON"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
	cm, err := c.getConfigMap(ctx)
	require.NoError(t, err)
	assert.Equal(t, "not model JSON", cm.Data["foreign"])
}

func TestArtifactAcknowledgementBindingWithdrawsCopiedRequestLabels(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	require.NoError(t, c.InitializeNodeUID("new-node"))
	ctx := context.Background()
	requestKey, err := constants.ArtifactReadyLabelKey("other-model")
	require.NoError(t, err)
	_, err = client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, UID: "new-node", ResourceVersion: "1", Labels: map[string]string{requestKey: "stale", "unrelated": "preserved"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
	require.NoError(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
	node, err := client.CoreV1().Nodes().Get(ctx, c.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, node.Labels, requestKey)
	assert.Equal(t, "preserved", node.Labels["unrelated"])
}

func TestArtifactAcknowledgementWithdrawsUnchangedStateLabel(t *testing.T) {
	for _, state := range []ModelStateOnNode{Updating, Failed, Evicted, Deleted} {
		t.Run(string(state), func(t *testing.T) {
			model := createTestBaseModelCM()
			requestKey, err := constants.ArtifactReadyLabelKey(model.UID)
			require.NoError(t, err)
			labels := map[string]string{requestKey: "old-request"}
			if state != Deleted {
				labels[constants.GetBaseModelLabel(model.Namespace, model.Name)] = string(state)
			}
			client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1", Labels: labels}})
			n := NewNodeLabelReconciler("node", client, 1, zap.NewNop().Sugar())
			require.NoError(t, n.applyNodeLabelOperation(&NodeLabelOp{BaseModel: model, ModelStateOnNode: state}))
			node, err := client.CoreV1().Nodes().Get(context.Background(), "node", metav1.GetOptions{})
			require.NoError(t, err)
			assert.NotContains(t, node.Labels, requestKey)
		})
	}
}

func TestArtifactAcknowledgementRecoveryRetainsPersistedIdentity(t *testing.T) {
	c, _, _ := setupConfigMapTest(t)
	key := "default.basemodel.model"
	raw := `{"name":"model","modelUID":"old-uid","status":"Updating","artifactRehydrationID":"completed","hfArtifactKey":"parent"}`
	c.cacheCommittedConfigMapEntries(nil, map[string]string{key: raw}, key, "new-uid")
	require.NotNil(t, c.modelCache[key])
	assert.EqualValues(t, "old-uid", c.modelCache[key].ModelUID)
	assert.Equal(t, "completed", c.modelCache[key].ArtifactRehydrationID)
	models, _ := c.cachedConfigMapEntries()
	var entry ModelEntry
	require.NoError(t, json.Unmarshal([]byte(models[key]), &entry))
	assert.EqualValues(t, "old-uid", entry.ModelUID)
	assert.Equal(t, "completed", entry.ArtifactRehydrationID)
}

func TestArtifactAcknowledgementPinsProcessNodeIdentity(t *testing.T) {
	c, client, _ := setupConfigMapTest(t)
	ctx := context.Background()
	require.NoError(t, c.InitializeNodeUID("old-node"))
	require.Error(t, c.InitializeNodeUID("new-node"))
	_, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.nodeName, UID: "new-node"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	model := createTestBaseModelCM()
	model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
	require.Error(t, c.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: model, ModelStatus: ModelStatusReady}))
	n := NewNodeLabelReconciler(c.nodeName, client, 1, zap.NewNop().Sugar())
	require.NoError(t, n.InitializeNodeUID("old-node"))
	require.Error(t, n.InitializeNodeUID("new-node"))
	require.Error(t, n.applyNodeLabelOperation(&NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready, NodeUID: "new-node"}))
}
