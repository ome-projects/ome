package modelagent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestScoutEvictionRespectsPlacement(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, tc := range []struct {
			name                     string
			oldEligible, newEligible bool
			want                     GopherTaskType
		}{
			{"eligible", true, true, Evict}, {"newly eligible", false, true, Evict},
			{"removed", true, false, Delete}, {"ineligible", false, false, ""},
		} {
			t.Run(map[bool]string{false: "BaseModel/", true: "ClusterBaseModel/"}[cluster]+tc.name, func(t *testing.T) {
				scout, tasks := newScoutForUpdateTest(t)
				old := newBaseModel("model", v1beta1.ReuseIfExists, "hf://org/model")
				old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				if tc.oldEligible {
					old.Spec.Storage.NodeAffinity = scoutTestNodeAffinity(scout.nodeName)
				}
				model := old.DeepCopy()
				model.Annotations = map[string]string{artifactResidencyAnnotation: "Evicted"}
				model.Spec.Storage.NodeAffinity = scoutTestNodeAffinity("other-node")
				if tc.newEligible {
					model.Spec.Storage.NodeAffinity = scoutTestNodeAffinity(scout.nodeName)
				}
				checkScoutUpdateTask(t, scout, tasks, cluster, old, model, tc.want)
				if tc.want != "" {
					require.Empty(t, tasks)
				}
				if cluster {
					scout.downloadClusterBaseModel(&v1beta1.ClusterBaseModel{ObjectMeta: model.ObjectMeta, Spec: model.Spec})
				} else {
					scout.downloadBaseModel(model)
				}
				if tc.newEligible {
					require.Len(t, tasks, 1)
					require.Equal(t, Evict, (<-tasks).TaskType)
				} else {
					require.Empty(t, tasks)
				}
			})
		}
	}
}

func newUnstartedEvictionFixture(t *testing.T, state string) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	t.Helper()
	s, task, input := newTestHfArtifactGopher(t)
	ctx := context.Background()
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	cm.Data = map[string]string{"unrelated": `{"name":"other","status":"Ready"}`}
	if state == "updating" {
		_, err = writeModelEntry(cm.Data, input.ChildModelKey, ModelEntry{Name: task.BaseModel.Name, Status: ModelStatusUpdating})
		require.NoError(t, err)
	}
	_, err = s.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	if state == "missing ConfigMap" {
		require.NoError(t, s.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{}))
	}
	s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.configMapReconciler.kubeClient, s.logger)
	s.hfArtifactHandler.repository.configMaps = s.configMapReconciler
	task.TaskType = Evict
	task.BaseModel.Annotations[artifactResidencyAnnotation] = "Evicted"
	label, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
	require.NoError(t, err)
	s.kubeClient = s.configMapReconciler.kubeClient
	_, err = s.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: s.configMapReconciler.nodeName, Labels: map[string]string{label: string(Ready), "unrelated": "preserve"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	s.nodeLabelReconciler = NewNodeLabelReconciler(s.configMapReconciler.nodeName, s.kubeClient, 1, s.logger)
	WithArtifactEviction(omefake.NewSimpleClientset(task.BaseModel.DeepCopy()))(s)
	return s, task, input
}

func TestEvictionBeforeDownloadReportsEvicted(t *testing.T) {
	for _, state := range []string{"missing entry", "missing ConfigMap", "updating"} {
		t.Run(state, func(t *testing.T) {
			s, task, input := newUnstartedEvictionFixture(t, state)
			download := *task
			download.TaskType, download.Sequence = Download, 0
			s.enqueueTask(&download)
			s.enqueueTask(task)
			require.Equal(t, 1, s.taskQueue.len(), "eviction replaces the queued Download")
			queued, ok := s.taskQueue.popHighPriority()
			require.True(t, ok)
			require.NoError(t, s.processTask(queued))
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusEvicted, entry.Status)
			require.Equal(t, types.UID(getModelUID(task)), entry.ModelUID)
			require.Empty(t, entry.HfArtifactKey)
			if state != "missing ConfigMap" {
				require.Equal(t, `{"name":"other","status":"Ready"}`, cm.Data["unrelated"])
			}
			node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			label, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
			require.NoError(t, err)
			require.NotContains(t, node.Labels, label)
			require.Equal(t, "preserve", node.Labels["unrelated"])
		})
	}
}

func TestEvictionMissingOwnershipPreservesLocalPath(t *testing.T) {
	for _, kind := range []string{"directory", "dangling symlink"} {
		t.Run(kind, func(t *testing.T) {
			s, task, input := newUnstartedEvictionFixture(t, "missing entry")
			if kind == "directory" {
				require.NoError(t, os.MkdirAll(input.ChildModelPath, 0755))
				require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "weights"), []byte("preserve"), 0600))
			} else {
				require.NoError(t, os.Symlink(input.Parent.LocalPath, input.ChildModelPath))
			}
			require.Error(t, s.processTask(task))
			_, err := os.Lstat(input.ChildModelPath)
			require.NoError(t, err)
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			require.NotContains(t, cm.Data, input.ChildModelKey)
		})
	}
}

func TestEvictionMissingEntryDoesNotDiscardSharedReference(t *testing.T) {
	s, task, input := newUnstartedEvictionFixture(t, "missing entry")
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	parent := input.Parent
	parent.Children = map[string]string{input.ChildModelKey: input.ChildModelPath}
	parent.Status = HfArtifactStatusReady
	raw, err := json.Marshal(parent)
	require.NoError(t, err)
	cm.Data[parent.Key] = string(raw)
	_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Error(t, s.processTask(task))
	after, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.NotContains(t, after.Data, input.ChildModelKey)
	require.Equal(t, string(raw), after.Data[parent.Key])
}

func TestEvictionBeforeDownloadAcrossModelKindsAndSources(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, source := range []struct {
			name, uri string
			policy    v1beta1.DownloadPolicy
		}{
			{"Shared OCI", "", v1beta1.ReuseIfExists},
			{"Shared HF", "hf://org/model", v1beta1.ReuseIfExists},
			{"Direct OCI", "oci://n/ns/b/bucket/o/model", v1beta1.AlwaysDownload},
			{"Direct HF", "hf://org/model", v1beta1.AlwaysDownload},
		} {
			t.Run(map[bool]string{false: "BaseModel/", true: "ClusterBaseModel/"}[cluster]+source.name, func(t *testing.T) {
				s, task, _ := newUnstartedEvictionFixture(t, "missing entry")
				if source.uri != "" {
					task.BaseModel.Spec.Storage.StorageUri = ptr(source.uri)
				}
				task.BaseModel.Spec.Storage.DownloadPolicy = ptr(source.policy)
				if cluster {
					model := task.BaseModel
					task.BaseModel = nil
					task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: model.Name, UID: model.UID, Annotations: model.Annotations}, Spec: model.Spec}
					WithArtifactEviction(omefake.NewSimpleClientset(task.ClusterBaseModel.DeepCopy()))(s)
				} else {
					WithArtifactEviction(omefake.NewSimpleClientset(task.BaseModel.DeepCopy()))(s)
				}
				require.NoError(t, s.processTask(task))
				cm, err := s.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, task.ClusterBaseModel))
				require.NoError(t, err)
				require.Equal(t, ModelStatusEvicted, entry.Status)
			})
		}
	}
}

func TestEmptyEvictionDefaultDestination(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/models/o/model"} {
		for _, empty := range []bool{false, true} {
			t.Run(uri+map[bool]string{false: "/omitted", true: "/empty"}[empty], func(t *testing.T) {
				s, task, input := newUnstartedEvictionFixture(t, "missing entry")
				task.BaseModel.Spec.Storage.StorageUri = ptr(uri)
				task.BaseModel.Spec.Storage.Path = nil
				if empty {
					task.BaseModel.Spec.Storage.Path = ptr("")
				}
				WithArtifactEviction(omefake.NewSimpleClientset(task.BaseModel.DeepCopy()))(s)
				require.NoError(t, s.processTask(task))
				cm, err := s.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				require.Equal(t, ModelStatusEvicted, entry.Status)
			})
		}
	}
}

func TestEmptyEvictionRetainsCachedOwnership(t *testing.T) {
	s, task, input := newUnstartedEvictionFixture(t, "missing entry")
	ctx := context.Background()
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	_, err = writeModelEntry(cm.Data, input.ChildModelKey, ModelEntry{Name: task.BaseModel.Name, ModelUID: input.ChildModelUID, Status: ModelStatusReady, HfArtifactKey: input.Parent.Key})
	require.NoError(t, err)
	_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = s.configMapReconciler.getConfigMapForRecovery(ctx)
	require.NoError(t, err)
	models, _ := s.configMapReconciler.cachedConfigMapEntries()
	require.Contains(t, models, input.ChildModelKey)
	delete(cm.Data, input.ChildModelKey)
	_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(input.ChildModelPath, 0755))
	sentinel := filepath.Join(input.ChildModelPath, "keep")
	require.NoError(t, os.WriteFile(sentinel, []byte("preserve"), 0600))
	require.ErrorContains(t, s.processTask(task), "recovery of recorded artifact ownership")
	after, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	require.NotContains(t, after.Data, input.ChildModelKey)
	content, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(content))
}

func TestEmptyEvictionRechecksOwnershipBeforeAcknowledgement(t *testing.T) {
	s, task, input := newUnstartedEvictionFixture(t, "missing entry")
	ctx := context.Background()
	injected := false
	client := s.kubeClient.(*fake.Clientset)
	client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		resource := corev1.SchemeGroupVersion.WithResource("configmaps")
		obj, err := client.Tracker().Get(resource, s.configMapReconciler.namespace, s.configMapReconciler.nodeName)
		require.NoError(t, err)
		cm := obj.(*corev1.ConfigMap).DeepCopy()
		_, err = writeModelEntry(cm.Data, input.ChildModelKey, ModelEntry{Name: task.BaseModel.Name, ModelUID: input.ChildModelUID, Status: ModelStatusUpdating, HfArtifactKey: input.Parent.Key})
		require.NoError(t, err)
		require.NoError(t, client.Tracker().Update(resource, cm, cm.Namespace))
		injected = true
		return false, nil, nil
	})
	require.ErrorContains(t, s.processTask(task), "ownership changed before eviction acknowledgement")
	require.True(t, injected)
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Equal(t, ModelStatusUpdating, entry.Status)
	require.Equal(t, input.Parent.Key, entry.HfArtifactKey)
}
