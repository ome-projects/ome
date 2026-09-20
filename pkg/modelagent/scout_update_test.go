package modelagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestScoutUpdatePlacementTransitions(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		kind := "BaseModel"
		if cluster {
			kind = "ClusterBaseModel"
		}
		for _, source := range []struct {
			name   string
			uri    string
			policy *v1beta1.DownloadPolicy
		}{
			{"OCI/default", "oci://n/ns/b/models/o/model", nil},
			{"OCI/always", "oci://n/ns/b/models/o/model", ptr(v1beta1.AlwaysDownload)},
			{"OCI/reuse", "oci://n/ns/b/models/o/model", ptr(v1beta1.ReuseIfExists)},
			{"HF/default", "hf://org/model", nil},
			{"HF/always", "hf://org/model", ptr(v1beta1.AlwaysDownload)},
			{"HF/reuse", "hf://org/model", ptr(v1beta1.ReuseIfExists)},
		} {
			for _, placement := range []string{"NodeAffinity", "NodeSelector"} {
				for _, tc := range []struct {
					name        string
					oldEligible bool
					newEligible bool
					want        GopherTaskType
				}{
					{"eligible_to_eligible", true, true, ""},
					{"eligible_to_ineligible", true, false, Delete},
					{"ineligible_to_eligible", false, true, Download},
					{"ineligible_to_ineligible", false, false, ""},
				} {
					t.Run(kind+"/"+source.name+"/"+placement+"/"+tc.name, func(t *testing.T) {
						scout, tasks := newScoutForUpdateTest(t)
						oldModel := newBaseModel("model", v1beta1.AlwaysDownload, source.uri)
						oldModel.Spec.Storage.DownloadPolicy = source.policy
						newModel := oldModel.DeepCopy()
						if placement == "NodeAffinity" {
							oldNode, newNode := "other-node", "another-node"
							if tc.oldEligible {
								oldNode = scout.nodeName
							}
							if tc.newEligible {
								newNode = scout.nodeName
							}
							oldModel.Spec.Storage.NodeAffinity = scoutTestNodeAffinity(oldNode)
							// Changing the node list must not refresh nodes that remain eligible.
							newModel.Spec.Storage.NodeAffinity = scoutTestNodeAffinity(newNode, "extra-node")
						} else {
							oldModel.Spec.Storage.NodeSelector = map[string]string{"gpu-model": "a100"}
							newModel.Spec.Storage.NodeSelector = map[string]string{"gpu-model": "h100"}
							if tc.oldEligible {
								oldModel.Spec.Storage.NodeSelector = nil
							}
							if tc.newEligible {
								newModel.Spec.Storage.NodeSelector = map[string]string{"gpu-model": "a10"}
							}
						}
						require.Equal(t, tc.oldEligible, scout.shouldDownloadModel(oldModel.Spec.Storage))
						require.Equal(t, tc.newEligible, scout.shouldDownloadModel(newModel.Spec.Storage))
						checkScoutUpdateTask(t, scout, tasks, cluster, oldModel, newModel, tc.want)
					})
				}
			}
		}
	}
}

func TestScoutUpdateAffinityPreservesRefreshAndDeletion(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		kind := "BaseModel"
		if cluster {
			kind = "ClusterBaseModel"
		}
		for _, tc := range []struct {
			name   string
			change func(old, new *v1beta1.BaseModel)
			want   GopherTaskType
		}{
			{"add_matching_affinity", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.NodeAffinity = nil
			}, ""},
			{"remove_matching_affinity", func(old, new *v1beta1.BaseModel) {
				new.Spec.Storage.NodeAffinity = nil
			}, ""},
			{"preferred_affinity_only", func(old, new *v1beta1.BaseModel) {
				new.Spec.Storage.NodeAffinity = old.Spec.Storage.NodeAffinity.DeepCopy()
				new.Spec.Storage.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.PreferredSchedulingTerm{{
					Weight: 1,
					Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "gpu-model", Operator: corev1.NodeSelectorOpIn, Values: []string{"a10"},
					}}},
				}}
			}, ""},
			{"source_change", func(old, new *v1beta1.BaseModel) {
				new.Spec.Storage.StorageUri = ptr("oci://n/ns/b/models/o/updated-model")
			}, DownloadOverride},
			{"path_change", func(old, new *v1beta1.BaseModel) {
				new.Spec.Storage.Path = ptr("/models/updated-model")
			}, DownloadOverride},
			{"credentials_change", func(old, new *v1beta1.BaseModel) {
				new.Spec.Storage.StorageKey = ptr("updated-secret")
			}, DownloadOverride},
			{"parameters_change", func(old, new *v1beta1.BaseModel) {
				new.Spec.Storage.Parameters = &map[string]string{"region": "us-chicago-1"}
			}, DownloadOverride},
			{"schema_change", func(old, new *v1beta1.BaseModel) {
				new.Spec.Storage.SchemaPath = ptr("config.json")
			}, DownloadOverride},
			{"label_refresh", func(old, new *v1beta1.BaseModel) {
				new.Labels = map[string]string{"refresh": "1"}
			}, DownloadOverride},
			{"annotation_refresh", func(old, new *v1beta1.BaseModel) {
				new.Annotations = map[string]string{"refresh": "1"}
			}, DownloadOverride},
			{"label_refresh_without_reuse_policy", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.DownloadPolicy = nil
				new.Spec.Storage.DownloadPolicy = nil
				new.Labels = map[string]string{"refresh": "1"}
			}, DownloadOverride},
			{"annotation_refresh_without_reuse_policy", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.DownloadPolicy = nil
				new.Spec.Storage.DownloadPolicy = nil
				new.Annotations = map[string]string{"refresh": "1"}
			}, DownloadOverride},
			{"newly_eligible_with_source_change", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				new.Spec.Storage.StorageUri = ptr("oci://n/ns/b/models/o/updated-model")
			}, Download},
			{"newly_eligible_tensorrt_model", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				new.Spec.ModelFormat.Name = constants.TensorRTLLM
				new.Spec.AdditionalMetadata = map[string]string{"type": "custom"}
			}, Download},
			{"still_ineligible_with_annotation_change", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				new.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("another-node")
				new.Annotations = map[string]string{"refresh": "1"}
			}, ""},
			{"matching_affinity_but_selector_excludes_node", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				old.Spec.Storage.NodeSelector = map[string]string{"gpu-model": "h100"}
				new.Spec.Storage.NodeSelector = map[string]string{"gpu-model": "h100"}
			}, ""},
			{"deletion_when_newly_eligible", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				now := metav1.Now()
				new.DeletionTimestamp = &now
			}, Delete},
			{"deletion_while_ineligible", func(old, new *v1beta1.BaseModel) {
				old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				new.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("another-node")
				now := metav1.Now()
				new.DeletionTimestamp = &now
			}, Delete},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				scout, tasks := newScoutForUpdateTest(t)
				oldModel := newBaseModel("model", v1beta1.ReuseIfExists, "oci://n/ns/b/models/o/model")
				oldModel.Spec.Storage.NodeAffinity = scoutTestNodeAffinity(scout.nodeName)
				newModel := oldModel.DeepCopy()
				newModel.Spec.Storage.NodeAffinity = scoutTestNodeAffinity(scout.nodeName, "extra-node")
				tc.change(oldModel, newModel)
				checkScoutUpdateTask(t, scout, tasks, cluster, oldModel, newModel, tc.want)
			})
		}
	}
}

func TestScoutNewlyEligibleDownloadDoesNotDependOnNodeAPI(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		kind := "BaseModel"
		if cluster {
			kind = "ClusterBaseModel"
		}
		t.Run(kind, func(t *testing.T) {
			scout, tasks := newScoutForUpdateTest(t)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, "node API unavailable", http.StatusServiceUnavailable)
			}))
			t.Cleanup(server.Close)
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			require.NoError(t, err)
			scout.kubeClient = client
			cachedNode := scout.nodeInfo.DeepCopy()
			oldModel := newBaseModel("model", v1beta1.ReuseIfExists, "oci://n/ns/b/models/o/model")
			oldModel.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
			newModel := oldModel.DeepCopy()
			newModel.Spec.Storage.NodeAffinity = scoutTestNodeAffinity(scout.nodeName)

			checkScoutUpdateTask(t, scout, tasks, cluster, oldModel, newModel, Download)
			assert.Zero(t, requests.Load(), "the update already evaluated eligibility using cached node information")
			assert.Equal(t, cachedNode, scout.nodeInfo)
		})
	}
}

func TestScoutAddEventDownloadEligibility(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		kind := "BaseModel"
		if cluster {
			kind = "ClusterBaseModel"
		}
		for _, tc := range []struct {
			name         string
			change       func(*v1beta1.BaseModel)
			nodeAPIFails bool
			wantRequests int32
			wantDownload bool
		}{
			{name: "eligible", wantRequests: 1, wantDownload: true},
			{name: "tensorrt_metadata", change: func(model *v1beta1.BaseModel) {
				model.Spec.ModelFormat.Name = constants.TensorRTLLM
				model.Spec.AdditionalMetadata = map[string]string{"type": "custom"}
			}, wantRequests: 1, wantDownload: true},
			{name: "deleting", change: func(model *v1beta1.BaseModel) {
				now := metav1.Now()
				model.DeletionTimestamp = &now
			}},
			{name: "affinity_excludes_node", change: func(model *v1beta1.BaseModel) {
				model.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
			}},
			{name: "selector_excludes_node", change: func(model *v1beta1.BaseModel) {
				model.Spec.Storage.NodeSelector = map[string]string{"gpu-model": "h100"}
			}},
			{name: "pvc_is_managed_by_controller", change: func(model *v1beta1.BaseModel) {
				model.Spec.Storage.StorageUri = ptr("pvc://model-pvc/model")
			}},
			{name: "node_api_unavailable", nodeAPIFails: true, wantRequests: 1},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				scout, tasks := newScoutForUpdateTest(t)
				refreshedNode := scout.nodeInfo.DeepCopy()
				refreshedNode.ResourceVersion = "2"
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					assert.Equal(t, http.MethodGet, r.Method)
					assert.Equal(t, "/api/v1/nodes/"+scout.nodeName, r.URL.Path)
					if tc.nodeAPIFails {
						http.Error(w, "node API unavailable", http.StatusServiceUnavailable)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					assert.NoError(t, json.NewEncoder(w).Encode(refreshedNode))
				}))
				t.Cleanup(server.Close)
				client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
				require.NoError(t, err)
				scout.kubeClient = client
				model := newBaseModel("model", v1beta1.ReuseIfExists, "oci://n/ns/b/models/o/model")
				if tc.change != nil {
					tc.change(model)
				}
				snapshot := model.DeepCopy()
				if cluster {
					scout.downloadClusterBaseModel(&v1beta1.ClusterBaseModel{ObjectMeta: model.ObjectMeta, Spec: model.Spec})
				} else {
					scout.downloadBaseModel(model)
				}
				assert.Equal(t, snapshot, model, "must not mutate the informer object")
				assert.Equal(t, tc.wantRequests, requests.Load())
				if !tc.wantDownload {
					require.Empty(t, tasks)
					return
				}
				require.Len(t, tasks, 1)
				task := <-tasks
				assert.Equal(t, Download, task.TaskType)
				if cluster {
					require.NotNil(t, task.ClusterBaseModel)
					assert.Equal(t, model.Spec, task.ClusterBaseModel.Spec)
					assert.Nil(t, task.BaseModel)
				} else {
					assert.Same(t, model, task.BaseModel)
					assert.Nil(t, task.ClusterBaseModel)
				}
				modelType := string(constants.ServingBaseModel)
				if model.Spec.AdditionalMetadata != nil {
					modelType = model.Spec.AdditionalMetadata["type"]
				}
				assert.Equal(t, &TensorRTLLMShapeFilter{
					IsTensorrtLLMModel: model.Spec.ModelFormat.Name == constants.TensorRTLLM,
					ShapeAlias:         scout.nodeShapeAlias,
					ModelType:          modelType,
				}, task.TensorRTLLMShapeFilter)
				assert.Equal(t, refreshedNode, scout.nodeInfo)
			})
		}
	}
}

func scoutTestNodeAffinity(names ...string) *corev1.NodeAffinity {
	return &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{
			Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: names,
		}}}},
	}}
}

func newScoutForUpdateTest(t *testing.T) (*Scout, chan *GopherTask) {
	t.Helper()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test-node", Labels: map[string]string{"gpu-model": "a10"}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/nodes/"+node.Name {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(node); err != nil {
			t.Errorf("encode node response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	tasks := make(chan *GopherTask, 10)
	return &Scout{ctx: context.Background(), nodeName: node.Name, nodeInfo: node.DeepCopy(), nodeShapeAlias: "a10",
		gopherChan: tasks, kubeClient: client, logger: zap.NewNop().Sugar()}, tasks
}

func checkScoutUpdateTask(t *testing.T, scout *Scout, tasks chan *GopherTask, cluster bool, oldModel, newModel *v1beta1.BaseModel, want GopherTaskType) {
	t.Helper()
	oldSnapshot, newSnapshot := oldModel.DeepCopy(), newModel.DeepCopy()
	if cluster {
		oldCluster := &v1beta1.ClusterBaseModel{ObjectMeta: oldModel.ObjectMeta, Spec: oldModel.Spec}
		newCluster := &v1beta1.ClusterBaseModel{ObjectMeta: newModel.ObjectMeta, Spec: newModel.Spec}
		scout.updateClusterBaseModel(oldCluster, newCluster)
	} else {
		scout.updateBaseModel(oldModel, newModel)
	}
	assert.Equal(t, oldSnapshot, oldModel, "must not mutate the old informer object")
	assert.Equal(t, newSnapshot, newModel, "must not mutate the new informer object")
	if want == "" {
		require.Empty(t, tasks)
		return
	}
	require.Len(t, tasks, 1)
	task := <-tasks
	assert.Equal(t, want, task.TaskType)
	if cluster {
		require.NotNil(t, task.ClusterBaseModel)
		assert.Equal(t, newModel.Spec, task.ClusterBaseModel.Spec)
	} else {
		assert.Same(t, newModel, task.BaseModel)
	}
	if want == Download {
		require.NotNil(t, task.TensorRTLLMShapeFilter)
		assert.Equal(t, scout.nodeShapeAlias, task.TensorRTLLMShapeFilter.ShapeAlias)
		assert.Equal(t, newModel.Spec.ModelFormat.Name == constants.TensorRTLLM, task.TensorRTLLMShapeFilter.IsTensorrtLLMModel)
		modelType := string(constants.ServingBaseModel)
		if value, ok := newModel.Spec.AdditionalMetadata["type"]; ok {
			modelType = value
		}
		assert.Equal(t, modelType, task.TensorRTLLMShapeFilter.ModelType)
	}
}
