package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDirectEvictionRequiresPersistedPathOwnership(t *testing.T) {
	for _, config := range []*ModelConfig{nil, {ModelType: "model"}} {
		g, task, path := newEvictionTestModel(t)
		cm, err := g.configMapReconciler.getConfigMap(context.Background())
		require.NoError(t, err)
		entry := directArtifactEntry(t, g, task)
		entry.Config = config
		_, err = writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
		require.NoError(t, err)
		_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
		require.NoError(t, err)
		_ = g.processTask(task)
		require.FileExists(t, filepath.Join(path, "weights"))
		require.Equal(t, ModelStatusReady, directArtifactEntry(t, g, task).Status)
	}
}

func TestCompletedEvictionDoesNotOwnNewDesiredPath(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprint(restart), func(t *testing.T) {
			g, task, _ := newEvictionTestModel(t)
			require.NoError(t, g.processTask(task))
			if restart {
				g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
			}
			unowned := filepath.Join(g.modelRootDir, "unowned")
			require.NoError(t, os.Mkdir(unowned, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(unowned, "weights"), []byte("unowned"), 0600))
			task.BaseModel.Spec.Storage.Path = &unowned
			_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			_ = g.processTask(task)
			require.FileExists(t, filepath.Join(unowned, "weights"))
		})
	}
}

func TestDirectPublicationPersistsPathBeforeReady(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/models/o/model"} {
		t.Run(uri, func(t *testing.T) {
			g, task, path := newDirectArtifactTestModel(t)
			task.BaseModel.Spec.Storage.StorageUri = &uri
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			ctx, release := withDirectArtifactDownloadOperation(context.Background())
			defer release()
			_, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
			require.NoError(t, err)
			require.True(t, acquired)
			client := g.kubeClient.(*k8sfake.Clientset)
			client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), g.configMapReconciler.namespace, g.configMapReconciler.nodeName)
				require.NoError(t, err)
				var entry map[string]interface{}
				require.NoError(t, json.Unmarshal([]byte(obj.(*corev1.ConfigMap).Data[getModelID(task.BaseModel, nil)]), &entry))
				require.Equal(t, path, entry["directArtifactPath"])
				return false, nil, nil
			})
			// Force a node Ready write, which must follow durable path publication.
			node, err := client.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)] = "Updating"
			_, err = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			published, err := g.finishDownloadStatus(ctx, task, &NodeLabelOp{ModelStateOnNode: Ready, BaseModel: task.BaseModel})
			require.NoError(t, err)
			require.True(t, published)
		})
	}
}

func TestOrdinaryHfWriterCanEvictWithoutParsedMetadata(t *testing.T) {
	g, task, _, source, downloads := newTestDirectHfSource(t)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	task.BaseModel.Annotations = map[string]string{ConfigParsingAnnotation: "true"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	g.kubeClient = g.configMapReconciler.kubeClient
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
	_, err := g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	ctx, release := withDirectArtifactDownloadOperation(context.Background())
	defer release()
	waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	require.False(t, waiting)
	require.Equal(t, 1, *downloads)
	ready, err := g.finishDownloadStatus(ctx, task, &NodeLabelOp{ModelStateOnNode: Ready, BaseModel: task.BaseModel})
	require.NoError(t, err)
	require.True(t, ready)
	require.Nil(t, directArtifactEntry(t, g, task).Config)
	release()
	task.TaskType = Evict
	task.BaseModel.Annotations[ArtifactResidencyAnnotation] = string(ModelStatusEvicted)
	_, err = g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	// Restart ensures eviction depends on durable ownership, not writer memory.
	g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
	g.artifactRouting.known = true
	require.NoError(t, g.processTask(task))
	require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path)
	require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
}

func TestDirectEvictionRetainsModelAndWithdrawsReadiness(t *testing.T) {
	g, task, path := newEvictionTestModel(t)
	client := g.kubeClient.(*k8sfake.Clientset)
	withdrawn := false
	client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		require.DirExists(t, path, "readiness must be withdrawn before deleting files")
		obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), g.configMapReconciler.namespace, g.configMapReconciler.nodeName)
		require.NoError(t, err)
		entry, err := existingModelEntry(obj.(*corev1.ConfigMap).Data, getModelID(task.BaseModel, nil))
		require.NoError(t, err)
		require.NotEqual(t, ModelStatusReady, entry.Status)
		require.Equal(t, &DirectArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path}, entry.DirectArtifactPendingEviction)
		withdrawn = true
		return false, nil, nil
	})
	require.NoError(t, g.processTask(task))
	require.True(t, withdrawn)
	require.NoDirExists(t, path)
	entry := directArtifactEntry(t, g, task)
	require.Equal(t, ModelStatusEvicted, entry.Status)
	require.Empty(t, entry.DirectArtifactPath, "completed eviction retains history without an active path claim")
	require.Nil(t, entry.DirectArtifactPendingEviction)
	require.Nil(t, entry.Config)
	_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(context.Background(), task.BaseModel.Name, metav1.GetOptions{})
	require.NoError(t, err)
	// Relist or duplicate delivery is idempotent; it must not download again.
	require.NoError(t, g.processTask(task))
	task.TaskType = Download
	require.NoError(t, g.processTask(task))
	require.NoDirExists(t, path)
	require.EqualValues(t, "Evicted", directArtifactEntry(t, g, task).Status)
}

func TestDirectEvictionRetriesAfterRestart(t *testing.T) {
	g, task, path := newEvictionTestModel(t)
	client := g.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("node unavailable")
	})
	require.Error(t, g.processTask(task))
	require.DirExists(t, path)
	require.NotEqual(t, ModelStatusReady, directArtifactEntry(t, g, task).Status)
	// A restarted reconciler has no in-memory ownership/cache information.
	g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, client, g.logger)
	client.ReactionChain = client.ReactionChain[1:]
	require.NoError(t, g.processTask(task))
	require.NoDirExists(t, path)
	require.EqualValues(t, "Evicted", directArtifactEntry(t, g, task).Status)
}

func TestDeleteResumesOwnedDirectEviction(t *testing.T) {
	for _, tc := range []struct {
		state    string
		complete bool
	}{
		{state: "pending"},
		{state: "evicted", complete: true},
		{state: "replacement"},
		{state: "absent"},
		{state: "placement loss"},
		{state: "placement loss", complete: true},
		{state: "replacement", complete: true},
	} {
		t.Run(fmt.Sprintf("%s/complete=%t", tc.state, tc.complete), func(t *testing.T) {
			g, task, path := newEvictionTestModel(t)
			client := g.kubeClient.(*k8sfake.Clientset)
			if !tc.complete {
				client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("node unavailable")
				})
				require.Error(t, g.processTask(task))
				client.ReactionChain = client.ReactionChain[1:]
			} else {
				require.NoError(t, g.processTask(task))
			}
			unowned := filepath.Join(g.modelRootDir, "unowned")
			require.NoError(t, os.Mkdir(unowned, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(unowned, "weights"), []byte("replacement"), 0600))
			deleting := task.BaseModel.DeepCopy()
			deleting.Spec.Storage.Path = &unowned
			if !tc.complete {
				deleting.Spec.Storage.StorageUri = stringPtr("local://" + unowned)
			}
			if tc.state == "placement loss" {
				deleting.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "another-node"}
			} else {
				now := metav1.Now()
				deleting.DeletionTimestamp = &now
			}
			live := deleting.DeepCopy()
			if tc.state == "replacement" {
				live.UID = "replacement"
				live.DeletionTimestamp = nil
			}
			g.modelClient = omefake.NewSimpleClientset(live)
			if tc.state == "absent" {
				g.modelClient = omefake.NewSimpleClientset()
			}
			g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, client, g.logger)
			task.BaseModel, task.TaskType = deleting, Delete
			before := directArtifactEntry(t, g, task)
			err := g.processTask(task)
			require.FileExists(t, filepath.Join(unowned, "weights"))
			if tc.state == "replacement" {
				require.Error(t, err)
				if !tc.complete {
					require.FileExists(t, filepath.Join(path, "weights"))
				}
				require.Equal(t, before, directArtifactEntry(t, g, task))
				return
			}
			require.NoError(t, err)
			require.NoDirExists(t, path)
			cm, err := g.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			require.NotContains(t, cm.Data, getModelID(deleting, nil))
		})
	}
}

func TestDeletePendingDirectEvictionProtectsReferences(t *testing.T) {
	for _, reference := range []string{"local borrower", "persisted path", "persisted alias", "active operation"} {
		t.Run(reference, func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newEvictionTestModel(t)
			require.NoError(t, g.persistDirectEviction(ctx, task, path, false))
			before := directArtifactEntry(t, g, task)
			other := task.BaseModel.DeepCopy()
			other.Name, other.UID, other.Annotations = "other", "other", nil
			wantError := "another model references eviction path"
			switch reference {
			case "local borrower":
				other.Spec.Storage.StorageUri, other.Spec.Storage.Path = stringPtr("local://"+path), nil
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
				require.NoError(t, err)
				wantError = "artifact directory is also referenced by another model"
			case "persisted path", "persisted alias":
				key := getModelID(other, nil)
				entry := ModelEntry{Name: other.Name, ModelUID: other.UID, Status: ModelStatusReady, DirectArtifactPath: path}
				if reference == "persisted alias" {
					alias := filepath.Join(t.TempDir(), "alias")
					require.NoError(t, os.Symlink(path, alias))
					entry.DirectArtifactPath = ""
					entry.Config = &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{key: alias}}}
				}
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				_, err = writeModelEntry(cm.Data, key, entry)
				require.NoError(t, err)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "active operation":
				release, acquired, err := g.acquireDirectArtifactPathLock(path)
				require.NoError(t, err)
				require.True(t, acquired)
				defer release()
				wantError = "has an active writer"
			}
			unowned := filepath.Join(g.modelRootDir, "new-desired-path")
			require.NoError(t, os.Mkdir(unowned, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(unowned, "weights"), []byte("unowned"), 0600))
			task.BaseModel.Spec.Storage.Path = &unowned
			task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "another-node"}
			_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			task.TaskType = Delete
			g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
			handled, err := g.resumeDirectEvictionOnDelete(ctx, task)
			require.True(t, handled)
			require.ErrorContains(t, err, wantError)
			require.FileExists(t, filepath.Join(path, "weights"))
			require.FileExists(t, filepath.Join(unowned, "weights"))
			require.Equal(t, before, directArtifactEntry(t, g, task))
		})
	}
}

func TestCompletedEvictionAllowsAnotherModelToReusePath(t *testing.T) {
	for _, tc := range []struct {
		operation  GopherTaskType
		legacyPath bool
	}{{Delete, false}, {Evict, false}, {Delete, true}, {Evict, true}} {
		t.Run(fmt.Sprintf("%s/legacyPath=%t", tc.operation, tc.legacyPath), func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newEvictionTestModel(t)
			require.NoError(t, g.processTask(task))
			other := task.BaseModel.DeepCopy()
			other.Name, other.UID, other.Annotations = "other", "other", nil
			_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, os.Mkdir(path, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("other model"), 0600))
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			otherKey := getModelID(other, nil)
			otherEntry := ModelEntry{Name: other.Name, ModelUID: other.UID, Status: ModelStatusReady, DirectArtifactPath: path}
			_, err = writeModelEntry(cm.Data, otherKey, otherEntry)
			require.NoError(t, err)
			if tc.legacyPath {
				// Older completed entries retained the path, but no longer own it.
				entry := directArtifactEntry(t, g, task)
				entry.DirectArtifactPath = path
				_, err = writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
				require.NoError(t, err)
			}
			_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
			require.NoError(t, err)
			task.TaskType = tc.operation
			if tc.operation == Delete {
				task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "another-node"}
				_, err = g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			// A restarted process must distinguish completed acknowledgement from
			// outstanding cleanup without relying on its in-memory task history.
			g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
			require.NoError(t, g.processTask(task))
			content, err := os.ReadFile(filepath.Join(path, "weights"))
			require.NoError(t, err)
			require.Equal(t, "other model", string(content))
			cm, err = g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			actualOther, err := existingModelEntry(cm.Data, otherKey)
			require.NoError(t, err)
			require.Equal(t, otherEntry, actualOther)
			if tc.operation == Delete {
				require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil))
			} else {
				require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
			}
		})
	}
}

func TestDirectEvictionResumesAfterFilesRemoved(t *testing.T) {
	for _, loseConfigMap := range []bool{false, true} {
		t.Run(fmt.Sprint(loseConfigMap), func(t *testing.T) {
			g, task, path := newEvictionTestModel(t)
			client := g.kubeClient.(*k8sfake.Clientset)
			client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
				cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
				require.NoError(t, err)
				if entry.Status == ModelStatusEvicted {
					require.NoDirExists(t, path, "Evicted must only publish after local cleanup")
					return true, nil, fmt.Errorf("final status unavailable")
				}
				return false, nil, nil
			})
			require.Error(t, g.processTask(task))
			require.NoDirExists(t, path)
			entry := directArtifactEntry(t, g, task)
			require.Equal(t, ModelStatusEvicting, entry.Status)
			require.Equal(t, path, entry.DirectArtifactPendingEviction.Path)
			client.ReactionChain = client.ReactionChain[1:]
			if loseConfigMap {
				// Same-process recovery must retain the receipt, never restore Ready.
				require.NoError(t, client.CoreV1().ConfigMaps(g.configMapReconciler.namespace).Delete(context.Background(), g.configMapReconciler.nodeName, metav1.DeleteOptions{}))
				g.configMapReconciler.reconcileConfigMaps()
				require.Equal(t, ModelStatusEvicting, directArtifactEntry(t, g, task).Status)
			} else {
				g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, client, g.logger)
			}
			require.NoError(t, g.processTask(task))
			require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
			require.Nil(t, directArtifactEntry(t, g, task).DirectArtifactPendingEviction)
		})
	}
}

func TestDirectEvictionClusterModel(t *testing.T) {
	g, task, path := newEvictionTestModel(t)
	cluster := &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
	cluster.Namespace = ""
	entry := directArtifactEntry(t, g, task)
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	delete(cm.Data, getModelID(task.BaseModel, nil))
	task.BaseModel, task.ClusterBaseModel = nil, cluster
	entry.Config.Artifact.ParentPath = map[string]string{getModelID(nil, cluster): path}
	_, err = writeModelEntry(cm.Data, getModelID(nil, cluster), entry)
	require.NoError(t, err)
	_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	g.modelClient = omefake.NewSimpleClientset(cluster)
	require.NoError(t, g.processTask(task))
	require.NoDirExists(t, path)
	require.Equal(t, ModelStatusEvicted, directArtifactEntry(t, g, task).Status)
	_, err = g.modelClient.OmeV1beta1().ClusterBaseModels().Get(context.Background(), cluster.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestDirectEvictionRejectsUnsafeOwnership(t *testing.T) {
	for _, scenario := range []string{"replacement", "persisted replacement", "unowned", "shared", "symlink", "outside", "path lock", "sibling", "persisted sibling", "ancestor", "outside ancestor", "filesystem ancestor", "local", "nested lock", "nested shared store", "linked sibling", "outside linked sibling"} {
		t.Run(scenario, func(t *testing.T) {
			g, task, path := newEvictionTestModel(t)
			ctx := context.Background()
			switch scenario {
			case "replacement":
				live := task.BaseModel.DeepCopy()
				live.UID = "replacement"
				g.modelClient = omefake.NewSimpleClientset(live)
			case "persisted replacement", "unowned", "shared":
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry := directArtifactEntry(t, g, task)
				switch scenario {
				case "persisted replacement":
					entry.ModelUID = "replacement"
				case "unowned":
					entry.ModelUID = ""
				case "shared":
					entry.Config = &ModelConfig{Artifact: Artifact{ChildrenPaths: []string{path + "-child"}}}
				}
				_, err = writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
				require.NoError(t, err)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "symlink":
				link := filepath.Join(g.modelRootDir, "link")
				require.NoError(t, os.Symlink(path, link))
				task.BaseModel.Spec.Storage.Path = &link
			case "outside":
				task.BaseModel.Spec.Storage.Path = stringPtr(t.TempDir())
			case "path lock":
				release, acquired, err := g.acquireDirectArtifactPathLock(path)
				require.NoError(t, err)
				require.True(t, acquired)
				defer release()
			case "sibling", "ancestor", "outside ancestor", "filesystem ancestor":
				other := task.BaseModel.DeepCopy()
				other.Name, other.UID = "other", "other"
				if scenario == "ancestor" {
					other.Spec.Storage.Path = &g.modelRootDir
				}
				if scenario == "outside ancestor" {
					other.Spec.Storage.Path = stringPtr(filepath.Dir(g.modelRootDir))
				}
				if scenario == "filesystem ancestor" {
					other.Spec.Storage.Path = stringPtr(string(filepath.Separator))
				}
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
				require.NoError(t, err)
			case "local":
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("local://" + path)
			case "persisted sibling":
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				_, err = writeModelEntry(cm.Data, "other.basemodel.other", ModelEntry{Name: "other", ModelUID: "other", DirectArtifactPath: path})
				require.NoError(t, err)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "nested lock", "nested shared store":
				reserved := hfArtifactLockDirectory
				if scenario == "nested shared store" {
					reserved = constants.ModelArtifactsDirectory
				}
				require.NoError(t, os.MkdirAll(filepath.Join(path, "nested", reserved), 0700))
			case "linked sibling", "outside linked sibling":
				other := task.BaseModel.DeepCopy()
				other.Name, other.UID = "other", "other"
				link := filepath.Join(g.modelRootDir, "other")
				if scenario == "outside linked sibling" {
					link = filepath.Join(t.TempDir(), "other")
				}
				require.NoError(t, os.Symlink(path, link))
				other.Spec.Storage.Path = &link
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			if scenario == "symlink" || scenario == "outside" || scenario == "local" {
				_, err := g.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			_ = g.processTask(task)
			require.FileExists(t, filepath.Join(path, "weights"))
			require.NotEqualValues(t, "Evicted", directArtifactEntry(t, g, task).Status)
		})
	}
}

func TestScoutRoutesEvictionBeforePlacement(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		g, task, _ := newEvictionTestModel(t)
		ch := make(chan *GopherTask, 2)
		scout := &Scout{logger: g.logger, gopherChan: ch, nodeInfo: &corev1.Node{}}
		task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"unmatched": "node"}
		if cluster {
			model := &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
			scout.downloadClusterBaseModel(model)
			scout.updateClusterBaseModel(model.DeepCopy(), model)
		} else {
			scout.downloadBaseModel(task.BaseModel)
			scout.updateBaseModel(task.BaseModel.DeepCopy(), task.BaseModel)
		}
		require.Len(t, ch, 2)
		for len(ch) != 0 {
			require.EqualValues(t, "Evict", (<-ch).TaskType)
		}
	}
}

func TestEvictionPreemptsPendingDownload(t *testing.T) {
	g, task, _ := newEvictionTestModel(t)
	queue := newGopherTaskQueue()
	download := *task
	download.TaskType = Download
	queue.enqueue(&download)
	queue.enqueue(task)
	require.Empty(t, queue.normalDownload)
	require.Len(t, queue.high, 1)
	ctx, cancel := context.WithCancel(context.Background())
	g.taskTracker.beginLegacyTask(gopherTaskModelKey(task), cancel)
	g.enqueueTask(task)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestOrdinaryDeleteFencesPersistedUIDAfterRestart(t *testing.T) {
	g, task, _ := newDirectArtifactTestModel(t)
	before := directArtifactEntry(t, g, task)
	task.BaseModel.UID = "stale"
	require.Error(t, g.configMapReconciler.DeleteModelFromConfigMap(context.Background(), task.BaseModel, nil))
	require.Equal(t, before, directArtifactEntry(t, g, task))
	node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Ready", node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
}
