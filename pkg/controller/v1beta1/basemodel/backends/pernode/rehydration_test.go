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

func TestRestorationConflictDoesNotReuseChangedInputs(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, change := range []string{"uid", "request", "placement", "source", "residency"} {
			for _, write := range []string{"spec", "status"} {
				t.Run(fmt.Sprintf("cluster=%t/%s/%s", cluster, change, write), func(t *testing.T) {
					obj, node := restorationModel(cluster), restorationNode("node-a")
					entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", NodeUID: node.UID, Status: shared.ModelStatusReady}
					if write == "spec" {
						entry.Config = &shared.ModelConfig{ModelType: "old-source"}
					}
					live := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
					changed := false
					changeInput := func() error {
						latest := obj.DeepCopyObject().(client.Object)
						require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(obj), latest))
						spec, _, err := shared.ModelSpecAndStatus(latest)
						require.NoError(t, err)
						switch change {
						case "uid":
							latest.SetUID("replacement")
						case "request":
							latest.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] = "new-request"
						case "placement":
							spec.Storage.NodeSelector["pool"] = "other"
						case "source":
							uri := "hf://new/source"
							spec.Storage.StorageUri = &uri
						case "residency":
							latest.GetAnnotations()[constants.ModelArtifactResidencyAnnotation] = "Evicted"
						}
						require.NoError(t, live.Update(context.Background(), latest))
						changed = true
						return apierrors.NewConflict(schema.GroupResource{Resource: "models"}, obj.GetName(), fmt.Errorf("changed input"))
					}
					c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
						Update: func(ctx context.Context, c client.WithWatch, current client.Object, opts ...client.UpdateOption) error {
							if write == "spec" && !changed {
								return changeInput()
							}
							return c.Update(ctx, current, opts...)
						},
						SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, current client.Object, opts ...client.SubResourceUpdateOption) error {
							if write == "status" && !changed {
								return changeInput()
							}
							return c.SubResource(sub).Update(ctx, current, opts...)
						},
					})
					require.Error(t, ReconcileStatusFromConfigMaps(context.Background(), c, live, logr.Discard(), obj, cluster, "Model"))
					require.True(t, changed)
					require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
					spec, status, err := shared.ModelSpecAndStatus(obj)
					require.NoError(t, err)
					require.Nil(t, spec.ModelType)
					require.Empty(t, status.NodesReady)
					require.Nil(t, status.Rehydration)
				})
			}
		}
	}
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
	entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", NodeUID: node.UID, Status: shared.ModelStatusReady, Config: &shared.ModelConfig{ModelType: "model-type"}}
	live := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
	c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
		Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			return apierrors.NewForbidden(schema.GroupResource{Resource: "basemodels"}, obj.GetName(), fmt.Errorf("optional config rejected"))
		},
	})
	require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, live, logr.Discard(), obj, false, "BaseModel"))
	require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
	spec, status, err := shared.ModelSpecAndStatus(obj)
	require.NoError(t, err)
	require.Nil(t, spec.ModelType)
	require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
	require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
}

func TestRestorationLookupFailurePreservesSnapshot(t *testing.T) {
	for _, fail := range []string{"nodes", "configmaps"} {
		t.Run(fail, func(t *testing.T) {
			obj := restorationModel(false)
			_, status, err := shared.ModelSpecAndStatus(obj)
			require.NoError(t, err)
			status.State, status.NodesReady = v1beta1.LifeCycleStateReady, []string{"previous"}
			status.Rehydration = &v1beta1.ModelRehydrationStatus{CompletedRequestID: "completed"}
			before := status.DeepCopy()
			live := restorationClient(t, obj)
			c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				_, nodes := list.(*corev1.NodeList)
				_, configmaps := list.(*corev1.ConfigMapList)
				if fail == "nodes" && nodes || fail == "configmaps" && configmaps {
					return fmt.Errorf("lookup failed")
				}
				return c.List(ctx, list, opts...)
			}})
			require.ErrorContains(t, ReconcileStatusFromConfigMaps(context.Background(), c, c, logr.Discard(), obj, false, "Model"), "lookup failed")
			require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
			_, status, err = shared.ModelSpecAndStatus(obj)
			require.NoError(t, err)
			require.Equal(t, before, status)
		})
	}
}

func TestRestorationHistorySurvivesNewRequestAndMissingReports(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprint(cluster), func(t *testing.T) {
			ctx := context.Background()
			obj, node := restorationModel(cluster), restorationNode("node-a")
			entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", NodeUID: node.UID, Status: shared.ModelStatusReady}
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

func TestRestorationCompletesCurrentRequestAcrossRetryAndNodeJoin(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprint(cluster), func(t *testing.T) {
			obj, node := restorationModel(cluster), restorationNode("node-a")
			_, initial, err := shared.ModelSpecAndStatus(obj)
			require.NoError(t, err)
			initial.Rehydration = &v1beta1.ModelRehydrationStatus{CompletedRequestID: "old"}
			entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", NodeUID: node.UID, Status: shared.ModelStatusReady}
			live := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
			changed := false
			c := interceptor.NewClient(live.(client.WithWatch), interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, current client.Object, opts ...client.SubResourceUpdateOption) error {
					if !changed {
						latest := obj.DeepCopyObject().(client.Object)
						require.NoError(t, live.Get(ctx, client.ObjectKeyFromObject(obj), latest))
						_, status, err := shared.ModelSpecAndStatus(latest)
						require.NoError(t, err)
						status.Rehydration.CompletedRequestID = "newer-persisted-completion"
						require.NoError(t, live.Status().Update(ctx, latest))
						require.NoError(t, live.Create(ctx, restorationNode("new-node")))
						changed = true
						return apierrors.NewConflict(schema.GroupResource{Resource: "models"}, obj.GetName(), fmt.Errorf("new completion"))
					}
					return c.SubResource(sub).Update(ctx, current, opts...)
				},
			})
			require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, live, logr.Discard(), obj, cluster, "Model"))
			require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
			_, status, err := shared.ModelSpecAndStatus(obj)
			require.NoError(t, err)
			require.Equal(t, []string{node.Name}, status.NodesReady)
			require.Equal(t, &v1beta1.ModelRehydrationStatus{CompletedRequestID: "restore-001"}, status.Rehydration)
		})
	}
}

func TestRestorationObservationsDoNotAcknowledgeNewRequest(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, observation := range []shared.ModelStatus{shared.ModelStatusEvicted, shared.ModelStatusEvicting, shared.ModelStatusFailed, shared.ModelStatusUpdating} {
			for _, ready := range []bool{false, true} {
				t.Run(fmt.Sprintf("cluster=%t/%s/ready=%t", cluster, observation, ready), func(t *testing.T) {
					obj, node := restorationModel(cluster), restorationNode("node-a")
					obj.GetAnnotations()[constants.ModelArtifactResidencyAnnotation] = "Evicted"
					entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "previous-request", NodeUID: node.UID, Status: observation, Config: &shared.ModelConfig{ModelType: "old-config"}}
					objects := []client.Object{obj, node, restorationReport(t, obj, node, entry)}
					if ready {
						other := restorationNode("node-b")
						entry.NodeUID, entry.ArtifactRehydrationID, entry.Status, entry.Config = other.UID, "restore-001", shared.ModelStatusReady, nil
						objects = append(objects, other, restorationReport(t, obj, other, entry))
					}
					c := restorationClient(t, objects...)
					require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, c, logr.Discard(), obj, cluster, "Model"))
					require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
					spec, status, err := shared.ModelSpecAndStatus(obj)
					require.NoError(t, err)
					expected := v1beta1.LifeCycleStateInTransit
					if observation == shared.ModelStatusEvicted {
						expected = v1beta1.LifeCycleStateEvicted
						require.Equal(t, []string{node.Name}, status.NodesEvicted)
					}
					if observation == shared.ModelStatusFailed {
						expected = v1beta1.LifeCycleStateFailed
					}
					if ready {
						expected = v1beta1.LifeCycleStateReady
					}
					require.Equal(t, expected, status.State)
					require.Nil(t, spec.ModelType)
					if ready {
						require.Equal(t, "restore-001", status.Rehydration.CompletedRequestID)
					} else {
						require.Empty(t, status.Rehydration.CompletedRequestID)
					}
				})
			}
		}
	}
}

func TestRestorationSnapshotRequiresCurrentIdentity(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, invalid := range []string{"", "modelUID", "requestID", "nodeUID"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, invalid), func(t *testing.T) {
				obj, node := restorationModel(cluster), restorationNode("node-a")
				entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", NodeUID: node.UID, Status: shared.ModelStatusReady, Config: &shared.ModelConfig{ModelType: "test-model"}}
				switch invalid {
				case "modelUID":
					entry.ModelUID = "old-model"
				case "requestID":
					entry.ArtifactRehydrationID = "old-request"
				case "nodeUID":
					entry.NodeUID = "old-node"
				}
				c := restorationClient(t, obj, node, restorationReport(t, obj, node, entry))
				require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, c, logr.Discard(), obj, cluster, "Model"))
				require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
				spec, status, err := shared.ModelSpecAndStatus(obj)
				require.NoError(t, err)
				if invalid != "" {
					require.Equal(t, v1beta1.LifeCycleStateInTransit, status.State)
					require.Nil(t, spec.ModelType, "stale report must not fill model configuration")
					return
				}
				require.Equal(t, v1beta1.LifeCycleStateReady, status.State)
				data, err := json.Marshal(status)
				require.NoError(t, err)
				var wire map[string]interface{}
				require.NoError(t, json.Unmarshal(data, &wire))
				require.Equal(t, map[string]interface{}{"completedRequestID": "restore-001"}, wire["rehydration"])
			})
		}
	}
}

func TestRestorationCompletesOnFirstEligibleReadyNode(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, second := range []string{"missing", "malformed", "Updating", "Failed", "Ready", "off-node", "zero-eligible"} {
			t.Run(fmt.Sprintf("cluster=%t/%s", cluster, second), func(t *testing.T) {
				obj, first, other := restorationModel(cluster), restorationNode("node-a"), restorationNode("node-b")
				entry := shared.ModelEntry{ModelUID: obj.GetUID(), ArtifactRehydrationID: "restore-001", NodeUID: first.UID, Status: shared.ModelStatusReady}
				objects := []client.Object{obj, first, other, restorationReport(t, obj, first, entry)}
				if second == "off-node" || second == "zero-eligible" {
					other.Labels["pool"] = "other"
				}
				if second == "zero-eligible" {
					first.Labels["pool"] = "other"
				}
				if second == "Ready" || second == "Failed" || second == "Updating" || second == "malformed" {
					entry.NodeUID, entry.Status = other.UID, shared.ModelStatus(second)
					cm := restorationReport(t, obj, other, entry)
					if second == "malformed" {
						for key := range cm.Data {
							cm.Data[key] = "{"
						}
					}
					objects = append(objects, cm)
				}
				c := restorationClient(t, objects...)
				require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, c, logr.Discard(), obj, cluster, "Model"))
				require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
				_, status, err := shared.ModelSpecAndStatus(obj)
				require.NoError(t, err)
				state := v1beta1.LifeCycleStateReady
				if second == "zero-eligible" {
					state = v1beta1.LifeCycleStateInTransit
				}
				require.Equal(t, state, status.State)
				data, err := json.Marshal(status)
				require.NoError(t, err)
				var wire struct {
					Rehydration struct {
						CompletedRequestID string
					}
				}
				require.NoError(t, json.Unmarshal(data, &wire))
				completed := ""
				if second != "zero-eligible" {
					completed = "restore-001"
				}
				require.Equal(t, completed, wire.Rehydration.CompletedRequestID)
			})
		}
	}
}
