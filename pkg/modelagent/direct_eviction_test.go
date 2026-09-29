package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/modelparser"
)

func newDirectEvictionFixture(t *testing.T, uri string) (*Gopher, *GopherTask) {
	t.Helper()
	s, task := newDirectOperationTestGopher(t, uri)
	task.TaskType = Evict
	task.BaseModel.Annotations = map[string]string{artifactResidencyAnnotation: "Evicted"}
	client := s.configMapReconciler.kubeClient.(*fake.Clientset)
	key, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
	require.NoError(t, err)
	_, err = client.CoreV1().Nodes().Update(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: s.configMapReconciler.nodeName, UID: "node-uid", Labels: map[string]string{key: string(Ready), "unrelated": "retained"},
	}}, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.kubeClient = client
	s.nodeLabelReconciler = NewNodeLabelReconciler(s.configMapReconciler.nodeName, client, 1, s.logger)
	WithArtifactEviction(omefake.NewSimpleClientset(task.BaseModel.DeepCopy()), "node-uid")(s)
	return s, task
}

func TestDirectEvictionRemovesFilesAndRetainsModel(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, uri)
			task.SharedArtifact = true // Source eligibility is not persisted ownership.
			before := task.BaseModel.DeepCopy()
			require.NoError(t, s.processTask(task))
			require.NoDirExists(t, *before.Spec.Storage.Path)
			current, err := s.omeClient.OmeV1beta1().BaseModels(before.Namespace).Get(context.Background(), before.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, before, current)
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
			require.NoError(t, err)
			require.Equal(t, ModelStatusEvicted, entry.Status)
			require.Equal(t, before.UID, entry.ModelUID)
			require.Empty(t, entry.Config.Artifact)
			// A completed acknowledgement cannot authorize deleting reused bytes.
			require.NoError(t, os.MkdirAll(*before.Spec.Storage.Path, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(*before.Spec.Storage.Path, "new"), []byte("new owner"), 0o600))
			task.Sequence = 0
			require.NoError(t, s.processTask(task))
			require.FileExists(t, filepath.Join(*before.Spec.Storage.Path, "new"))
		})
	}
}

func TestDirectEvictionOrdinaryOCIWithoutHfMetadata(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		key := getModelID(task.BaseModel, nil)
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		entry.Config.Artifact = Artifact{} // The ordinary OCI parser does not populate HF reuse metadata.
		return writeModelEntry(cm.Data, key, entry)
	}))
	require.NoError(t, s.processTask(task))
	require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path)
}

func TestDirectEvictionAfterOrdinaryOCIParserFailure(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	ctx := context.Background()
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	delete(cm.Data, getModelID(task.BaseModel, nil))
	_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.kubeClient, s.logger)
	WithArtifactEviction(s.omeClient, "node-uid")(s)
	task.TaskType = Download
	task.BaseModel.Annotations = nil
	_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.modelConfigParser = modelparser.NewModelConfigParser(nil, s.logger)
	writes := 0
	require.NoError(t, runDirectTestDownload(s, task, func(_ context.Context, path string) error {
		writes++
		// No config.json: the real parser fails, but ordinary OCI download
		// completion still emits a named Ready entry (its existing contract).
		return os.WriteFile(filepath.Join(path, "ordinary.bin"), []byte("bytes"), 0o600)
	}))
	require.Equal(t, 1, writes)
	cm, err = s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
	require.NoError(t, err)
	require.Equal(t, ModelStatusReady, entry.Status)
	require.Nil(t, entry.Config)
	task.TaskType, task.Sequence = Evict, 0
	task.BaseModel.Annotations = map[string]string{artifactResidencyAnnotation: "Evicted"}
	_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.processTask(task))
	require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path)
}

func TestDirectEvictionRejectsBorrowersAndUnsupportedPaths(t *testing.T) {
	for _, change := range []string{"direct borrower", "local borrower", "parsed borrower", "lookup", "withdrawal", "local", "outside", "symlink"} {
		t.Run(change, func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, "hf://org/model")
			path := *task.BaseModel.Spec.Storage.Path
			ctx := context.Background()
			switch change {
			case "direct borrower", "local borrower":
				borrower := task.BaseModel.DeepCopy()
				borrower.Name, borrower.UID = "borrower", "borrower-uid"
				borrower.Annotations = nil
				if change == "local borrower" {
					borrower.Spec.Storage.StorageUri = ptr("local://" + path)
					borrower.Spec.Storage.Path = nil
				}
				_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(ctx, borrower, metav1.CreateOptions{})
				require.NoError(t, err)
			case "parsed borrower":
				require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
					return writeModelEntry(cm.Data, "borrower", ModelEntry{Name: "borrower", Config: &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"owner": path}}}})
				}))
			case "lookup":
				s.omeClient.(*omefake.Clientset).PrependReactor("list", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("lookup failed") })
			case "withdrawal":
				s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("patch failed") })
			case "local":
				task.BaseModel.Spec.Storage.StorageUri = ptr("local://" + path)
			case "outside":
				task.BaseModel.Spec.Storage.Path = ptr(t.TempDir())
			case "symlink":
				link := filepath.Join(s.modelRootDir, "alias")
				require.NoError(t, os.Symlink(path, link))
				task.BaseModel.Spec.Storage.Path = &link
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.Error(t, s.processTask(task))
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestDirectEvictionReceiptSurvivesInterruptedCompletion(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	path := *task.BaseModel.Spec.Storage.Path
	client := s.kubeClient.(*fake.Clientset)
	failed := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		var entry ModelEntry
		_ = json.Unmarshal([]byte(cm.Data[getModelID(task.BaseModel, nil)]), &entry)
		if entry.Status == ModelStatusEvicted && !failed {
			failed = true
			return true, nil, errors.New("completion interrupted")
		}
		return false, nil, nil
	})
	require.ErrorContains(t, s.processTask(task), "completion interrupted")
	require.NoDirExists(t, path)
	// New process and changed destination: only the recorded old path is cleanup authority.
	s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, client, s.logger)
	WithArtifactEviction(s.omeClient, "node-uid")(s)
	task.BaseModel.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "new-destination"))
	require.NoError(t, os.MkdirAll(*task.BaseModel.Spec.Storage.Path, 0o755))
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	task.Sequence = 0
	require.NoError(t, s.processTask(task))
	require.DirExists(t, *task.BaseModel.Spec.Storage.Path)
}

func seedDirectCleanup(t *testing.T, s *Gopher, task *GopherTask) DirectArtifactPendingDeletion {
	t.Helper()
	path, err := s.directEvictionPath(*task.BaseModel.Spec.Storage.Path)
	require.NoError(t, err)
	pending := DirectArtifactPendingDeletion{ModelUID: task.BaseModel.UID, Path: path}
	require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		key := getModelID(task.BaseModel, nil)
		entry, err := existingModelEntry(cm.Data, key)
		if err != nil {
			return false, err
		}
		entry.ModelUID, entry.NodeUID = task.BaseModel.UID, s.artifactNodeUID
		entry.Status, entry.DirectArtifactPendingDeletion = ModelStatusUpdating, &pending
		return writeModelEntry(cm.Data, key, entry)
	}))
	return pending
}

func TestDirectCleanupOrdinaryDeleteUsesReceipt(t *testing.T) {
	for _, reserve := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "reserve"}[reserve], func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
			pending := seedDirectCleanup(t, s, task)
			task.TaskType = Delete
			task.BaseModel.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "new"))
			require.NoError(t, os.MkdirAll(*task.BaseModel.Spec.Storage.Path, 0o755))
			if reserve {
				task.BaseModel.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, s.processTask(task))
			if reserve {
				require.DirExists(t, pending.Path)
			} else {
				require.NoDirExists(t, pending.Path)
			}
			require.DirExists(t, *task.BaseModel.Spec.Storage.Path)
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil))
		})
	}
}

func TestDirectCleanupSharedRoutedDeleteUsesReceipt(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, uri)
			pending := seedDirectCleanup(t, s, task)
			task.TaskType, task.SharedArtifact = Delete, true
			require.NoError(t, s.processTask(task))
			require.NoDirExists(t, pending.Path)
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil))
		})
	}
}

func TestDirectCleanupBlocksForeignWriter(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	seedDirectCleanup(t, s, task)
	writer := *task
	writer.TaskType = Download
	writer.BaseModel = task.BaseModel.DeepCopy()
	writer.BaseModel.Name, writer.BaseModel.UID, writer.BaseModel.Annotations = "writer", "writer-uid", nil
	_, err := s.omeClient.OmeV1beta1().BaseModels(writer.BaseModel.Namespace).Create(context.Background(), writer.BaseModel, metav1.CreateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, writer.BaseModel}}
	require.Error(t, runDirectTestDownload(s, &writer, func(context.Context, string) error {
		t.Error("pending cleanup path was overwritten")
		return errors.New("writer reached")
	}))
}

func TestDirectCleanupBlocksForeignWriterThroughAlias(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	pending := seedDirectCleanup(t, s, task)
	writer := *task
	writer.TaskType, writer.BaseModel = Download, task.BaseModel.DeepCopy()
	writer.BaseModel.Name, writer.BaseModel.UID, writer.BaseModel.Annotations = "writer", "writer-uid", nil
	alias := filepath.Join(s.modelRootDir, "writer-alias")
	require.NoError(t, os.Symlink(pending.Path, alias))
	writer.BaseModel.Spec.Storage.Path = &alias
	_, err := s.omeClient.OmeV1beta1().BaseModels(writer.BaseModel.Namespace).Create(context.Background(), writer.BaseModel, metav1.CreateOptions{})
	require.NoError(t, err)
	s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, writer.BaseModel}}
	s.modelConfigParser = modelparser.NewModelConfigParser(nil, s.logger)
	require.ErrorContains(t, runDirectTestDownload(s, &writer, func(context.Context, string) error { t.Error("alias overwrote unfinished cleanup"); return nil }), "unfinished Direct")
}

func TestDirectCleanupRejectsReplacementModelAndNode(t *testing.T) {
	for _, change := range []string{"model", "node"} {
		t.Run(change, func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, "hf://org/model")
			pending := seedDirectCleanup(t, s, task)
			if change == "model" {
				task.BaseModel.UID = "replacement"
				_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
			} else {
				node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				node.UID = "replacement"
				_, err = s.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
				require.NoError(t, err)
				WithArtifactEviction(s.omeClient, "replacement")(s) // New agent, reused Node name.
			}
			task.TaskType = Delete
			require.Error(t, s.processTask(task))
			require.FileExists(t, filepath.Join(pending.Path, "weights"))
		})
	}
}

func TestDirectCleanupReceiptCacheRecovery(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "hf://org/model")
	pending := seedDirectCleanup(t, s, task)
	client := s.kubeClient.CoreV1().ConfigMaps(s.configMapReconciler.namespace)
	require.NoError(t, client.Delete(context.Background(), s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
	require.NoError(t, s.configMapReconciler.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: task.BaseModel, ModelStatus: ModelStatusUpdating}))
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
	require.NoError(t, err)
	require.Equal(t, &pending, entry.DirectArtifactPendingDeletion)
	require.Equal(t, s.artifactNodeUID, entry.NodeUID)
}

func TestDirectEvictionRequiresUnambiguousOwnership(t *testing.T) {
	for _, state := range []string{"absent", "malformed", "empty", "children", "old shared reference"} {
		t.Run(state, func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, "hf://org/model")
			path := *task.BaseModel.Spec.Storage.Path
			require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
				key := getModelID(task.BaseModel, nil)
				switch state {
				case "absent":
					delete(cm.Data, key)
				case "malformed":
					cm.Data[key] = "broken"
				case "empty":
					cm.Data[key] = "{}"
				case "children":
					entry, err := existingModelEntry(cm.Data, key)
					require.NoError(t, err)
					entry.Config.Artifact.ChildrenPaths = []string{filepath.Join(s.modelRootDir, "unreported-borrower")}
					return writeModelEntry(cm.Data, key, entry)
				case "old shared reference":
					parent := testHfArtifactEntry(t)
					parent.Children = map[string]string{key: path}
					return writeHfArtifactEntry(cm.Data, parent)
				}
				return true, nil
			}))
			require.Error(t, s.processTask(task))
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestDirectCleanupDownloadRejectsCurrentLocal(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "hf://org/model")
	pending := seedDirectCleanup(t, s, task)
	task.TaskType = Download
	task.BaseModel.Annotations = nil
	task.BaseModel.Spec.Storage.StorageUri = ptr("local://" + pending.Path)
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Error(t, s.processTask(task))
	require.FileExists(t, filepath.Join(pending.Path, "weights"))
}

func TestDirectCleanupDeletePreservesCurrentLocal(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "hf://org/model")
	pending := seedDirectCleanup(t, s, task)
	task.TaskType = Delete
	task.BaseModel.Spec.Storage.StorageUri = ptr("local://" + pending.Path)
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.processTask(task))
	require.FileExists(t, filepath.Join(pending.Path, "weights"))
}

func TestDirectCleanupOrdinaryDownloadFinishesOldPathFirst(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	pending := seedDirectCleanup(t, s, task)
	task.TaskType = Download
	task.BaseModel.Annotations = nil
	task.BaseModel.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "new"))
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	s.modelConfigParser = modelparser.NewModelConfigParser(nil, s.logger)
	writes := 0
	require.NoError(t, runDirectTestDownload(s, task, func(_ context.Context, path string) error {
		writes++
		require.NoDirExists(t, pending.Path)
		require.Equal(t, *task.BaseModel.Spec.Storage.Path, path)
		return os.MkdirAll(path, 0o755)
	}))
	require.Equal(t, 1, writes)
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
	require.NoError(t, err)
	require.Nil(t, entry.DirectArtifactPendingDeletion)
	require.NoError(t, s.kubeClient.CoreV1().ConfigMaps(s.configMapReconciler.namespace).Delete(context.Background(), s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
	require.NoError(t, s.configMapReconciler.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: task.BaseModel, ModelStatus: ModelStatusUpdating}))
	cm, err = s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err = existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
	require.NoError(t, err)
	require.Nil(t, entry.DirectArtifactPendingDeletion, "completed cleanup cannot reappear from cache")
}

func TestDirectEvictionWaitsForCancellationInsensitiveWriter(t *testing.T) {
	for _, uri := range []string{"hf://org/model", "oci://n/ns/b/bucket/o/model"} {
		t.Run(uri, func(t *testing.T) {
			s, evict := newDirectEvictionFixture(t, uri)
			writer := *evict
			writer.TaskType, writer.BaseModel = Download, evict.BaseModel.DeepCopy()
			writer.BaseModel.Annotations = nil
			_, err := s.omeClient.OmeV1beta1().BaseModels(writer.BaseModel.Namespace).Update(context.Background(), writer.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.modelConfigParser = modelparser.NewModelConfigParser(nil, s.logger)
			started, release, done := make(chan context.Context, 1), make(chan struct{}), make(chan error, 1)
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			go func() {
				done <- runDirectTestDownload(s, &writer, func(ctx context.Context, path string) error {
					started <- ctx
					<-release
					return os.WriteFile(filepath.Join(path, "late"), []byte("late"), 0o600)
				})
			}()
			var writerCtx context.Context
			select {
			case writerCtx = <-started:
			case err := <-done:
				t.Fatalf("writer did not start: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("writer did not start")
			}
			_, err = s.omeClient.OmeV1beta1().BaseModels(evict.BaseModel.Namespace).Update(context.Background(), evict.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, s.processTask(evict))
			require.ErrorIs(t, writerCtx.Err(), context.Canceled)
			require.Same(t, evict, receiveDirectRetry(t, s))
			require.DirExists(t, *evict.BaseModel.Spec.Storage.Path)
			unblock()
			require.ErrorIs(t, <-done, context.Canceled)
			require.NoError(t, s.processTask(evict))
			require.NoDirExists(t, *evict.BaseModel.Spec.Storage.Path)
		})
	}
}

func TestDirectEvictionInterruptedReceiptBoundaries(t *testing.T) {
	for _, stage := range []string{"before receipt", "lost receipt response", "before completion", "lost completion response"} {
		t.Run(stage, func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
			path, key := *task.BaseModel.Spec.Storage.Path, getModelID(task.BaseModel, nil)
			client := s.kubeClient.(*fake.Clientset)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, key)
				require.NoError(t, err)
				completion := stage == "before completion" || stage == "lost completion response"
				if injected || completion && entry.Status != ModelStatusEvicted || !completion && entry.DirectArtifactPendingDeletion == nil {
					return false, nil, nil
				}
				injected = true
				if stage == "lost receipt response" || stage == "lost completion response" {
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, action.GetNamespace()))
				}
				return true, nil, errors.New("interrupted " + stage)
			})
			require.ErrorContains(t, s.processTask(task), "interrupted")
			if stage == "before receipt" || stage == "lost receipt response" {
				require.FileExists(t, filepath.Join(path, "weights"))
			} else {
				require.NoDirExists(t, path)
			}
			if stage == "lost completion response" {
				require.NoError(t, os.MkdirAll(path, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(path, "replacement"), []byte("new bytes"), 0o600))
			}
			s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, client, s.logger)
			WithArtifactEviction(s.omeClient, "node-uid")(s)
			task.Sequence = 0
			require.NoError(t, s.processTask(task))
			if stage == "lost completion response" {
				require.FileExists(t, filepath.Join(path, "replacement"))
			} else {
				require.NoDirExists(t, path)
			}
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, key)
			require.NoError(t, err)
			require.Nil(t, entry.DirectArtifactPendingDeletion)
			require.Equal(t, ModelStatusEvicted, entry.Status)
		})
	}
}

func TestDirectEvictionConflictsRecheckCurrentAuthority(t *testing.T) {
	for _, change := range []string{"UID", "intent", "source", "path", "metadata", "node", "receipt"} {
		t.Run(change, func(t *testing.T) {
			s, task := newDirectEvictionFixture(t, "hf://org/model")
			client := s.kubeClient.(*fake.Clientset)
			path, key := *task.BaseModel.Spec.Storage.Path, getModelID(task.BaseModel, nil)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, key)
				require.NoError(t, err)
				if injected || entry.DirectArtifactPendingDeletion == nil {
					return false, nil, nil
				}
				injected = true
				model := task.BaseModel.DeepCopy()
				switch change {
				case "UID":
					model.UID = "replacement"
				case "intent":
					delete(model.Annotations, artifactResidencyAnnotation)
				case "source":
					model.Spec.Storage.StorageUri = ptr("hf://other/model")
				case "path":
					model.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "new"))
				case "metadata":
					model.Labels = map[string]string{"changed": "yes"}
				case "node":
					obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := obj.(*corev1.Node)
					node.UID = "replacement"
					err = client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, "")
					require.NoError(t, err)
				case "receipt":
					entry.DirectArtifactPendingDeletion.Path = filepath.Join(s.modelRootDir, "changed-receipt")
					_, err := writeModelEntry(cm.Data, key, entry)
					require.NoError(t, err)
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, action.GetNamespace()))
				}
				_, err = s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
				require.NoError(t, err)
				return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("changed authority"))
			})
			require.Error(t, s.processTask(task))
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestDirectPendingReceiptProtectsSharedCleanup(t *testing.T) {
	for _, pathKind := range []string{"parent", "child"} {
		t.Run(pathKind, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			path := input.Parent.LocalPath
			if pathKind == "child" {
				path = input.ChildModelPath
			}
			require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
				return writeModelEntry(cm.Data, "other", ModelEntry{Name: "other", ModelUID: "other-uid", NodeUID: s.artifactNodeUID, DirectArtifactPendingDeletion: &DirectArtifactPendingDeletion{ModelUID: "other-uid", Path: path}})
			}))
			require.Error(t, s.processTask(task))
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			require.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestDirectCleanupRecoversCachedAuthorityBeforeDelete(t *testing.T) {
	for _, state := range []string{"completed", "pending"} {
		for _, loss := range []string{"configmap", "entry"} {
			t.Run(state+"/"+loss, func(t *testing.T) {
				s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
				ctx := context.Background()
				path := *task.BaseModel.Spec.Storage.Path
				if state == "pending" {
					seedDirectCleanup(t, s, task)
					task.BaseModel.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "new-destination"))
					require.NoError(t, os.MkdirAll(*task.BaseModel.Spec.Storage.Path, 0o755))
					_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
					require.NoError(t, err)
				} else {
					require.NoError(t, s.processTask(task))
					require.NoError(t, os.MkdirAll(path, 0o755))
					require.NoError(t, os.WriteFile(filepath.Join(path, "replacement"), []byte("new bytes"), 0o600))
					borrower := task.BaseModel.DeepCopy()
					borrower.Name, borrower.UID, borrower.Annotations = "borrower", "borrower-uid", nil
					borrower.Spec.Storage.StorageUri, borrower.Spec.Storage.Path = ptr("local://"+path), nil
					_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(ctx, borrower, metav1.CreateOptions{})
					require.NoError(t, err)
					s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, borrower}}
				}
				client := s.kubeClient.CoreV1().ConfigMaps(s.configMapReconciler.namespace)
				if loss == "configmap" {
					require.NoError(t, client.Delete(ctx, s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
				} else {
					cm, err := client.Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
					require.NoError(t, err)
					delete(cm.Data, getModelID(task.BaseModel, nil))
					_, err = client.Update(ctx, cm, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				task.TaskType, task.Sequence = Delete, 0
				require.NoError(t, s.processTask(task))
				if state == "completed" {
					require.FileExists(t, filepath.Join(path, "replacement"))
				} else {
					require.NoDirExists(t, path)
					require.DirExists(t, *task.BaseModel.Spec.Storage.Path)
				}
				cm, err := s.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				require.NotContains(t, cm.Data, getModelID(task.BaseModel, nil))
			})
		}
	}
}

func TestDirectCleanupCachedReceiptBlocksWriterAfterEntryLoss(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	pending := seedDirectCleanup(t, s, task)
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	delete(cm.Data, getModelID(task.BaseModel, nil))
	_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	ctx, release := directFileOperationContext(context.Background())
	defer release()
	_, err = s.tryLockDirectModelPath(ctx, pending.Path)
	require.ErrorContains(t, err, "unfinished Direct artifact cleanup")
}

func TestSharedEvictionRejectsMixedDirectCleanupAuthority(t *testing.T) {
	for _, stage := range []string{"initial relationship", "pending shared receipt", "completion retry"} {
		t.Run(stage, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			ctx := context.Background()
			directPath := filepath.Join(s.modelRootDir, "other-direct")
			require.NoError(t, os.MkdirAll(directPath, 0o755))
			addDirect := func(cm *corev1.ConfigMap) {
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				entry.DirectArtifactPendingDeletion = &DirectArtifactPendingDeletion{ModelUID: task.BaseModel.UID, Path: directPath}
				entry.NodeUID = s.artifactNodeUID
				_, err = writeModelEntry(cm.Data, input.ChildModelKey, entry)
				require.NoError(t, err)
			}
			if stage == "completion retry" {
				client := s.kubeClient.(*fake.Clientset)
				injected := false
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
					require.NoError(t, err)
					if injected || entry.Status != ModelStatusEvicted {
						return false, nil, nil
					}
					injected = true
					obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
					require.NoError(t, err)
					current := obj.(*corev1.ConfigMap)
					addDirect(current)
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), current, cm.Namespace))
					return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("mixed cleanup ownership"))
				})
			} else {
				if stage == "pending shared receipt" {
					_, err := s.sharedHfArtifactHandler().repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
					require.NoError(t, err)
				}
				require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) { addDirect(cm); return true, nil }))
			}
			require.Error(t, s.processTask(task))
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.NotEqual(t, ModelStatusEvicted, entry.Status)
			require.NotNil(t, entry.DirectArtifactPendingDeletion)
			require.DirExists(t, directPath)
			if stage != "completion retry" {
				assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
				require.DirExists(t, input.Parent.LocalPath)
			}
		})
	}
}

func TestDirectCompletedDeleteFailureRetainsOnlyTerminalProof(t *testing.T) {
	for _, modelState := range []string{"selector delete", "deleting", "absent"} {
		for _, response := range []string{"failed", "lost"} {
			for _, loss := range []string{"configmap", "entry"} {
				t.Run(modelState+"/"+response+"/"+loss, func(t *testing.T) {
					s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
					ctx := context.Background()
					require.NoError(t, s.processTask(task))
					path, key := *task.BaseModel.Spec.Storage.Path, getModelID(task.BaseModel, nil)
					require.NoError(t, os.MkdirAll(path, 0o755))
					require.NoError(t, os.WriteFile(filepath.Join(path, "replacement"), []byte("new bytes"), 0o600))
					borrower := task.BaseModel.DeepCopy()
					borrower.Name, borrower.UID, borrower.Annotations = "borrower", "borrower-uid", nil
					borrower.Spec.Storage.StorageUri, borrower.Spec.Storage.Path = ptr("local://"+path), nil
					_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(ctx, borrower, metav1.CreateOptions{})
					require.NoError(t, err)
					s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, borrower}}
					if modelState != "selector delete" {
						now := metav1.Now()
						task.BaseModel.DeletionTimestamp = &now
						_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
						require.NoError(t, err)
						if modelState == "absent" {
							require.NoError(t, s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Delete(ctx, task.BaseModel.Name, metav1.DeleteOptions{}))
						}
					}
					client := s.kubeClient.(*fake.Clientset)
					fail := true
					client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
						cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
						if _, present := cm.Data[key]; present || !fail {
							return false, nil, nil
						}
						if response == "lost" {
							require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
						}
						return true, nil, errors.New("final removal interrupted")
					})
					task.TaskType, task.Sequence = Delete, 0
					require.ErrorContains(t, s.processTask(task), "final removal interrupted")
					require.FileExists(t, filepath.Join(path, "replacement"))
					fail = false
					cms := client.CoreV1().ConfigMaps(s.configMapReconciler.namespace)
					if loss == "configmap" {
						require.NoError(t, cms.Delete(ctx, s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
					} else {
						cm, err := cms.Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
						require.NoError(t, err)
						delete(cm.Data, key)
						_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
						require.NoError(t, err)
					}
					// Finalization proof is not a recoverable model or Ready entry.
					s.configMapReconciler.recreateConfigMap(ctx)
					if modelState != "selector delete" {
						require.True(t, s.configMapReconciler.isModelMutationBlocked(key, task.BaseModel.UID))
						require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: task.BaseModel, ModelStatus: ModelStatusReady}))
					}
					cm, err := cms.Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
					if !apierrors.IsNotFound(err) {
						require.NoError(t, err)
						require.NotContains(t, cm.Data, key)
					}
					task.Sequence = 0
					require.NoError(t, s.processTask(task))
					require.FileExists(t, filepath.Join(path, "replacement"))
					s.configMapReconciler.cacheMutex.RLock()
					_, cached := s.configMapReconciler.modelCache[key]
					s.configMapReconciler.cacheMutex.RUnlock()
					require.False(t, cached, "acknowledged final removal must clear terminal proof")
				})
			}
		}
	}
}

func TestDirectEarlyInvalidationIsNotCompletedCleanup(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	key := getModelID(task.BaseModel, nil)
	s.configMapReconciler.invalidateModelUIDAndEvictCache(key, task.BaseModel.UID)
	require.NoError(t, s.kubeClient.CoreV1().ConfigMaps(s.configMapReconciler.namespace).Delete(context.Background(), s.configMapReconciler.nodeName, metav1.DeleteOptions{}))
	task.TaskType = Delete
	require.NoError(t, s.processTask(task))
	require.NoDirExists(t, *task.BaseModel.Spec.Storage.Path, "ordinary deletion still removes files; invalidation alone proves no completed eviction")
}

func TestSharedCompletedEvictionOrdinaryDelete(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	require.NoError(t, s.processTask(task))
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.Empty(t, entry.NodeUID, "Shared eviction uses the common completed acknowledgement without layout or receipt Node identity")
	task.TaskType, task.Sequence = Delete, 0
	require.NoError(t, s.processTask(task))
	cm, err = s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	require.NotContains(t, cm.Data, input.ChildModelKey)
}
