package pernode

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func restorationModel(cluster bool) client.Object {
	meta := metav1.ObjectMeta{Name: "model", UID: "model-uid", Annotations: map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore-001"}}
	spec := v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{NodeSelector: map[string]string{"pool": "models"}}}
	if cluster {
		return &v1beta1.ClusterBaseModel{ObjectMeta: meta, Spec: spec}
	}
	meta.Namespace = "models"
	return &v1beta1.BaseModel{ObjectMeta: meta, Spec: spec}
}

func restorationNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), Labels: map[string]string{"pool": "models"}}}
}

func restorationReport(t *testing.T, obj client.Object, node *corev1.Node, entry shared.ModelEntry) *corev1.ConfigMap {
	t.Helper()
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: node.Name, Namespace: constants.OMENamespace, Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"}}, Data: map[string]string{constants.GetModelConfigMapKey(obj.GetNamespace(), obj.GetName(), obj.GetNamespace() == ""): string(data)}}
}

func restorationClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1beta1.BaseModel{}, &v1beta1.ClusterBaseModel{}).WithObjects(objects...).Build()
}

func TestRestorationStatusSurvivesOptionalConfigRejection(t *testing.T) {
	obj, node := restorationModel(false), restorationNode("node-a")
	entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", Status: shared.ModelStatusReady, Config: &shared.ModelConfig{ModelType: "model-type"}}
	live := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
	specUpdates := 0
	c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
		Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			specUpdates++
			return apierrors.NewForbidden(schema.GroupResource{Resource: "basemodels"}, obj.GetName(), fmt.Errorf("optional config rejected"))
		},
	})
	require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, live, logr.Discard(), obj, false, "BaseModel"))
	require.Positive(t, specUpdates, "the rejected spec update must be attempted")
	require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
	spec, status, err := shared.ModelSpecAndStatus(obj)
	require.NoError(t, err)
	require.Nil(t, spec.ModelType)
	require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
	require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
}

func TestRestorationHistorySurvivesNewRequestAndMissingReports(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprint(cluster), func(t *testing.T) {
			ctx := context.Background()
			obj, node := restorationModel(cluster), restorationNode("node-a")
			entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", Status: shared.ModelStatusReady}
			cm := restorationReport(t, obj, node, entry)
			c := restorationClient(t, obj, node, cm)
			reconcile := func() *v1beta1.ModelStatusSpec {
				require.NoError(t, ReconcileStatusFromConfigMaps(ctx, c, c, logr.Discard(), obj, cluster, "Model"))
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), obj))
				_, status, err := shared.ModelSpecAndStatus(obj)
				require.NoError(t, err)
				return status
			}
			require.Equal(t, "restore-001", reconcile().Rehydration.CompletedRequestID)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cm), cm))
			entry.Status = shared.ModelStatusUpdating
			cm.Data = restorationReport(t, obj, node, entry).Data
			require.NoError(t, c.Update(ctx, cm))
			require.NoError(t, c.Create(ctx, restorationNode("joining-node")))
			lostReady := reconcile()
			require.Equal(t, v1beta1.LifeCycleStateInTransit, lostReady.State)
			require.Empty(t, lostReady.NodesReady)
			require.Equal(t, "restore-001", lostReady.Rehydration.CompletedRequestID)
			obj.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] = "restore-002"
			require.NoError(t, c.Update(ctx, obj))
			status := reconcile()
			require.Equal(t, v1beta1.LifeCycleStateInTransit, status.State)
			require.Equal(t, &v1beta1.ModelRehydrationStatus{CompletedRequestID: "restore-001"}, status.Rehydration)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cm), cm))
			for key := range cm.Data {
				cm.Data[key] = "{"
			}
			require.NoError(t, c.Update(ctx, cm))
			require.Equal(t, "restore-001", reconcile().Rehydration.CompletedRequestID)
			entry.Status = shared.ModelStatusReady
			entry.ArtifactRehydrationID = "restore-002"
			cm.Data = restorationReport(t, obj, node, entry).Data
			require.NoError(t, c.Update(ctx, cm))
			require.Equal(t, "restore-002", reconcile().Rehydration.CompletedRequestID)
		})
	}
}

func TestLegacyReportsRemainCompatibleButRejectWrongUID(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, uid := range []types.UID{"", "model-uid", "old-model"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, uid), func(t *testing.T) {
				obj, node := restorationModel(cluster), restorationNode("node-a")
				obj.SetAnnotations(nil)
				entry := shared.ModelEntry{ModelUID: uid, Status: shared.ModelStatusReady, Config: &shared.ModelConfig{ModelType: "legacy"}}
				c := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
				require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, c, logr.Discard(), obj, cluster, "Model"))
				require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
				spec, status, err := shared.ModelSpecAndStatus(obj)
				require.NoError(t, err)
				require.Nil(t, status.Rehydration)
				if uid == "old-model" {
					require.Empty(t, status.NodesReady)
					require.Nil(t, spec.ModelType)
				} else {
					require.Equal(t, []string{node.Name}, status.NodesReady)
					require.Equal(t, "legacy", *spec.ModelType)
				}
			})
		}
	}
}

func TestRestorationAcknowledgement(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, observation := range []string{"current", "old-request", "old-model", "missing-model-uid", "missing-node", "Updating", "Failed", "Evicted"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, observation), func(t *testing.T) {
				obj, node := restorationModel(cluster), restorationNode("node-a")
				entry := shared.ModelEntry{ModelUID: obj.GetUID(), Status: shared.ModelStatusReady, ArtifactRehydrationID: "restore-001"}
				switch observation {
				case "old-request":
					entry.ArtifactRehydrationID = "previous"
				case "old-model":
					entry.ModelUID = "previous-model"
				case "missing-model-uid":
					entry.ModelUID = ""
				case "Updating", "Failed", "Evicted":
					entry.Status = shared.ModelStatus(observation)
				}
				cm := restorationReport(t, obj, node, entry)
				cm.UID = "report-uid"
				c := restorationClient(t, obj, node, cm)
				if observation == "missing-node" {
					require.NoError(t, c.Delete(t.Context(), node))
				}
				require.NoError(t, ReconcileStatusFromConfigMaps(t.Context(), c, c, logr.Discard(), obj, cluster, "Model"))
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
				_, status, err := shared.ModelSpecAndStatus(obj)
				require.NoError(t, err)
				if observation == "current" {
					require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
					wire, err := json.Marshal(status)
					require.NoError(t, err)
					var decoded map[string]interface{}
					require.NoError(t, json.Unmarshal(wire, &decoded))
					require.Equal(t, map[string]interface{}{"completedRequestID": "restore-001"}, decoded["rehydration"])
				} else {
					require.Nil(t, status.Rehydration)
				}
				if observation == "old-request" {
					require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
				}
			})
		}
	}
}

func TestRestorationConflictRecollectsCurrentRequest(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, change := range []string{"request", "model", "report", "matching-new-request"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, change), func(t *testing.T) {
				requireConflictRecollects(t, cluster, change)
			})
		}
	}
}

// rewriteRestorationReport replaces the node's report with the given entry.
func rewriteRestorationReport(ctx context.Context, t *testing.T, live client.Client, obj client.Object, node *corev1.Node, entry shared.ModelEntry) {
	t.Helper()
	cm := restorationReport(t, obj, node, entry)
	data := cm.Data
	require.NoError(t, live.Get(ctx, client.ObjectKeyFromObject(cm), cm))
	cm.Data = data
	require.NoError(t, live.Update(ctx, cm))
}

// changeRestorationInput changes the request, the Model or the node report
// the way a writer that races the status update would.
func changeRestorationInput(ctx context.Context, t *testing.T, live client.Client, obj client.Object, node *corev1.Node, entry *shared.ModelEntry, change string) {
	t.Helper()
	latest := obj.DeepCopyObject().(client.Object)
	require.NoError(t, live.Get(ctx, client.ObjectKeyFromObject(obj), latest))
	switch change {
	case "request", "matching-new-request":
		latest.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] = "restore-002"
		if change == "matching-new-request" {
			entry.ArtifactRehydrationID = "restore-002"
			rewriteRestorationReport(ctx, t, live, obj, node, *entry)
		}
	case "model":
		latest.SetUID("replacement")
	case "report":
		entry.Status = shared.ModelStatusFailed
		rewriteRestorationReport(ctx, t, live, obj, node, *entry)
	}
	require.NoError(t, live.Update(ctx, latest))
}

// requireConflictRecollects changes one input, fails the first status update
// with a conflict, and checks that the retry reads the reports again.
func requireConflictRecollects(t *testing.T, cluster bool, change string) {
	t.Helper()
	obj, node := restorationModel(cluster), restorationNode("node-a")
	entry := shared.ModelEntry{ModelUID: obj.GetUID(), Status: shared.ModelStatusReady, ArtifactRehydrationID: "restore-001"}
	live := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
	changed, lists := false, 0
	c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, current client.Object, opts ...client.SubResourceUpdateOption) error {
			if !changed {
				changeRestorationInput(ctx, t, live, obj, node, &entry, change)
				changed = true
				return apierrors.NewConflict(schema.GroupResource{Resource: "models"}, obj.GetName(), fmt.Errorf("input changed"))
			}
			return c.SubResource(sub).Update(ctx, current, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, reports := list.(*corev1.ConfigMapList); reports {
				lists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	err := ReconcileStatusFromConfigMaps(t.Context(), c, live, logr.Discard(), obj, cluster, "Model")
	if change == "model" {
		require.ErrorContains(t, err, "identity changed during reconciliation")
	} else {
		require.NoError(t, err)
	}
	require.True(t, changed)
	require.NoError(t, live.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
	_, status, err := shared.ModelSpecAndStatus(obj)
	require.NoError(t, err)
	if change == "matching-new-request" {
		require.NotNil(t, status.Rehydration)
		require.Equal(t, "restore-002", status.Rehydration.CompletedRequestID)
		require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
	} else {
		require.Nil(t, status.Rehydration, "stale reports must not establish completion")
	}
	if change != "model" {
		require.Equal(t, 2, lists, "a status conflict must refresh reports")
	}
	if change == "request" {
		require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
		require.Equal(t, []string{"node-a"}, status.NodesReady)
	}
	if change == "report" {
		require.Equal(t, v1beta1.LifeCycleStateFailed, status.State)
	}
}

func TestRestorationCompletesWithoutOtherNodes(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		obj, node := restorationModel(cluster), restorationNode("node-a")
		entry := shared.ModelEntry{ModelUID: obj.GetUID(), Status: shared.ModelStatusReady, ArtifactRehydrationID: "restore-001"}
		other := restorationNode("node-b")
		c := restorationClient(t, obj, node, other, restorationReport(t, obj, node, entry))
		require.NoError(t, ReconcileStatusFromConfigMaps(t.Context(), c, c, logr.Discard(), obj, cluster, "Model"))
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
		_, status, err := shared.ModelSpecAndStatus(obj)
		require.NoError(t, err)
		require.Equal(t, []string{"node-a"}, status.NodesReady)
		require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
	}
}

// The agent validates placement before acknowledgement. The controller
// aggregates reports without listing the fleet or duplicating Scout.
func TestRestorationDoesNotReevaluatePlacement(t *testing.T) {
	obj, node := restorationModel(false), restorationNode("node-a")
	node.Labels["pool"] = "changed-after-acknowledgement"
	entry := shared.ModelEntry{ModelUID: obj.GetUID(), Status: shared.ModelStatusReady, ArtifactRehydrationID: "restore-001"}
	live := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
	c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, nodes := list.(*corev1.NodeList); nodes {
				t.Fatal("restoration must not list all Nodes")
			}
			return c.List(ctx, list, opts...)
		},
	})
	require.NoError(t, ReconcileStatusFromConfigMaps(t.Context(), c, c, logr.Discard(), obj, false, "Model"))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
	_, status, err := shared.ModelSpecAndStatus(obj)
	require.NoError(t, err)
	require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
}

func TestRestorationReadsReportsOnce(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, withConfig := range []bool{false, true} {
			for _, withRequest := range []bool{false, true} {
				t.Run(fmt.Sprintf("cluster=%t/config=%t/request=%t", cluster, withConfig, withRequest), func(t *testing.T) {
					requireReportsReadOnce(t, cluster, withConfig, withRequest)
				})
			}
		}
	}
}

// requireReportsReadOnce reconciles one Ready report and checks that the
// reports are listed once and that no Node is read from the live reader.
func requireReportsReadOnce(t *testing.T, cluster, withConfig, withRequest bool) {
	t.Helper()
	obj, node := restorationModel(cluster), restorationNode("node-a")
	entry := shared.ModelEntry{ModelUID: obj.GetUID(), Status: shared.ModelStatusReady, ArtifactRehydrationID: "restore-001"}
	if withConfig {
		entry.Config = &shared.ModelConfig{ModelType: "test-type"}
	}
	if !withRequest {
		obj.SetAnnotations(nil)
	}
	live := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
	lists, nodeReads := 0, 0
	c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, reports := list.(*corev1.ConfigMapList); reports {
				lists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	reader := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, node := obj.(*corev1.Node); node {
				nodeReads++
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	require.NoError(t, ReconcileStatusFromConfigMaps(t.Context(), c, reader, logr.Discard(), obj, cluster, "Model"))
	require.Equal(t, 1, lists, "metadata and status must share the report read")
	require.Zero(t, nodeReads, "completion must not add live Node reads")
	require.NoError(t, live.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
	spec, status, err := shared.ModelSpecAndStatus(obj)
	require.NoError(t, err)
	require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
	if withConfig {
		require.Equal(t, "test-type", *spec.ModelType)
	}
	if withRequest {
		require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
	} else {
		require.Nil(t, status.Rehydration)
	}
}

func TestRestorationCompletionDoesNotReadLiveNode(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprint(cluster), func(t *testing.T) {
			obj, node := restorationModel(cluster), restorationNode("node-a")
			entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", Status: shared.ModelStatusReady}
			c := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
			liveReads := 0
			reader := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, isNode := obj.(*corev1.Node); isNode {
						liveReads++
						return apierrors.NewServiceUnavailable("live Node read unavailable")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			// Both initial acknowledgement and subsequent reconciles use the
			// reports that the cleanup of absent Nodes already filtered.
			for range 2 {
				require.NoError(t, ReconcileStatusFromConfigMaps(t.Context(), c, reader, logr.Discard(), obj, cluster, "Model"))
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
				_, status, err := shared.ModelSpecAndStatus(obj)
				require.NoError(t, err)
				require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
				require.NotNil(t, status.Rehydration)
				require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
			}
			require.Zero(t, liveReads)
		})
	}
}
