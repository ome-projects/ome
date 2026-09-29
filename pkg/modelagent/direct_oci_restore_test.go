package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

type testOCIRestoreStore struct {
	list      func(ociobjectstore.ObjectURI) ([]objectstorage.ObjectSummary, error)
	validate  localCopyValidator
	download  func(context.Context, []ociobjectstore.ObjectURI, string) error
	downloads int
}

func (s *testOCIRestoreStore) ListObjects(uri ociobjectstore.ObjectURI) ([]objectstorage.ObjectSummary, error) {
	return s.list(uri)
}
func (s *testOCIRestoreStore) IsLocalCopyValid(uri ociobjectstore.ObjectURI, path string) (bool, error) {
	return s.validate(uri, path)
}
func (s *testOCIRestoreStore) BulkDownloadContext(ctx context.Context, uris []ociobjectstore.ObjectURI, path string, _ int, _ ...ociobjectstore.DownloadOption) error {
	s.downloads++
	return s.download(ctx, uris, path)
}

func newDirectOCIRestoreFixture(t *testing.T) (*Gopher, *GopherTask, *testOCIRestoreStore) {
	t.Helper()
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	s.taskQueue = newGopherTaskQueue()
	s.modelVerificationLimiter = newVerificationLimiter(2)
	s.modelConfigParser = modelparser.NewModelConfigParser(nil, s.logger)
	task.TaskType = Download
	task.BaseModel.Annotations = map[string]string{testRestoreAnnotation: "request-1"}
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	store := &testOCIRestoreStore{
		list: func(uri ociobjectstore.ObjectURI) ([]objectstorage.ObjectSummary, error) {
			return []objectstorage.ObjectSummary{{Name: ptr(strings.TrimSuffix(uri.Prefix, "/") + "/weights")}}, nil
		},
		validate: func(_ ociobjectstore.ObjectURI, path string) (bool, error) {
			b, err := os.ReadFile(path)
			if os.IsNotExist(err) {
				return false, nil
			}
			return string(b) == "model", err
		},
		download: func(_ context.Context, uris []ociobjectstore.ObjectURI, path string) error {
			for _, uri := range uris {
				file := filepath.Join(path, ociobjectstore.TrimObjectPrefix(uri.ObjectName, uri.Prefix))
				if _, err := os.Stat(file); err == nil {
					continue
				} // override=false
				if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
					return err
				}
				if err := os.WriteFile(file, []byte("model"), 0o600); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return s, task, store
}

func runDirectOCIRestore(s *Gopher, task *GopherTask, store ociModelStore) error {
	return s.processTaskWithSourceAdapters(task, true, nil, func(ctx context.Context, uri *ociobjectstore.ObjectURI, path string, task *GopherTask) error {
		return s.downloadModelWithStore(ctx, uri, path, task, store)
	})
}

func TestDirectOCIRestoreValidatesConfiguredDirectory(t *testing.T) {
	for _, state := range []string{"healthy", "missing", "incomplete", "corrupt"} {
		t.Run(state, func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			path := *task.BaseModel.Spec.Storage.Path
			switch state {
			case "missing":
				require.NoError(t, os.RemoveAll(path))
			case "incomplete":
				require.NoError(t, os.Remove(filepath.Join(path, "weights")))
			case "corrupt":
				require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("wrong"), 0o600))
			}
			require.NoError(t, runDirectOCIRestore(s, task, store))
			if state == "healthy" {
				require.Zero(t, store.downloads)
			} else {
				require.Equal(t, 1, store.downloads)
			}
			b, err := os.ReadFile(filepath.Join(path, "weights"))
			require.NoError(t, err)
			require.Equal(t, "model", string(b))
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
			require.NoError(t, err)
			require.Equal(t, ModelStatusReady, entry.Status)
			require.Equal(t, "request-1", entry.ArtifactRehydrationID)
			require.Equal(t, task.BaseModel.UID, entry.ModelUID)
			require.Equal(t, s.artifactNodeUID, entry.NodeUID)
			require.Empty(t, entry.HfArtifactKey)
			node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, "request-1", node.Labels[testRequestLabel(task)])
		})
	}
}

func TestDirectOCIRestoreInspectionErrorsPreserveBytes(t *testing.T) {
	for _, failure := range []string{"list", "empty", "nil name", "unsafe", "inspection after invalid", "directory"} {
		t.Run(failure, func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			path := filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights")
			require.NoError(t, os.WriteFile(path, []byte("wrong"), 0o600))
			store.list = func(uri ociobjectstore.ObjectURI) ([]objectstorage.ObjectSummary, error) {
				switch failure {
				case "list":
					return nil, errors.New("authorization denied")
				case "empty":
					return nil, nil
				case "nil name":
					return []objectstorage.ObjectSummary{{}}, nil
				case "unsafe":
					return []objectstorage.ObjectSummary{{Name: ptr("model/weights")}, {Name: ptr("model/../outside")}}, nil
				default:
					return []objectstorage.ObjectSummary{{Name: ptr("model/weights")}, {Name: ptr("model/second")}}, nil
				}
			}
			if failure == "inspection after invalid" {
				validate := store.validate
				store.validate = func(uri ociobjectstore.ObjectURI, path string) (bool, error) {
					if uri.ObjectName == "model/second" {
						return false, errors.New("metadata unavailable")
					}
					return validate(uri, path)
				}
			}
			if failure == "directory" {
				require.NoError(t, os.Mkdir(filepath.Join(*task.BaseModel.Spec.Storage.Path, "second"), 0o755))
			}
			require.Error(t, runDirectOCIRestore(s, task, store))
			require.Zero(t, store.downloads)
			b, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "wrong", string(b))
		})
	}
}

func TestDirectOCIRestorePeriodicRecovery(t *testing.T) {
	s, _, store := newDirectOCIRestoreFixture(t)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Equal(t, 1, s.taskQueue.len())
	require.NoError(t, runDirectOCIRestore(s, popArtifactRecovery(t, s), store))
	require.Zero(t, store.downloads)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Zero(t, s.taskQueue.len())
}

func TestDirectOCIRestoreFilteredRecovery(t *testing.T) {
	for _, shape := range []string{"BM.GPU.H100.8", "unknown"} {
		t.Run(shape, func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			task.BaseModel.Spec.ModelFormat.Name = constants.TensorRTLLM
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			node.Labels[constants.NodeInstanceShapeLabel] = shape
			_, err = s.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
			require.NoError(t, err)
			store.list = func(ociobjectstore.ObjectURI) ([]objectstorage.ObjectSummary, error) {
				return []objectstorage.ObjectSummary{{Name: ptr("model/H100/weights")}, {Name: ptr("model/A100-80G/weights")}, {Name: ptr("model/config.json")}}, nil
			}
			validate := store.validate
			store.validate = func(uri ociobjectstore.ObjectURI, path string) (bool, error) {
				require.Equal(t, "model/H100/weights", uri.ObjectName)
				return validate(uri, path)
			}
			download := store.download
			store.download = func(ctx context.Context, uris []ociobjectstore.ObjectURI, path string) error {
				require.Len(t, uris, 1)
				require.Equal(t, "model/H100/weights", uris[0].ObjectName)
				return download(ctx, uris, path)
			}
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			recovery := popArtifactRecovery(t, s)
			if shape == "unknown" {
				require.ErrorContains(t, runDirectOCIRestore(s, recovery, store), "no suitable objects")
				require.Zero(t, store.downloads)
				return
			}
			require.Equal(t, "H100", recovery.TensorRTLLMShapeFilter.ShapeAlias)
			require.NoError(t, runDirectOCIRestore(s, recovery, store))
			require.Equal(t, 1, store.downloads)
			require.FileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "H100/weights"))
			require.NoFileExists(t, filepath.Join(*task.BaseModel.Spec.Storage.Path, "A100-80G/weights"))
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			require.Zero(t, s.taskQueue.len())
		})
	}
}

func TestDirectOCIRestoreDoesNotAdoptResidentSharedEligiblePath(t *testing.T) {
	s, task, store := newDirectOCIRestoreFixture(t)
	sha := strings.Repeat("a", 40)
	task.BaseModel.Annotations[hfModelIDAnnotationKey], task.BaseModel.Annotations[hfSHAAnnotationKey] = "org/model", sha
	task.BaseModel.Spec.Storage.StorageUri = ptr("oci://n/ns/b/bucket/o/org/model/" + sha)
	policy := v1beta1.ReuseIfExists
	task.BaseModel.Spec.Storage.DownloadPolicy = &policy
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, eligible, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, s.modelRootDir)
	require.NoError(t, err)
	require.True(t, eligible)
	task.SharedArtifact = true
	require.NoError(t, runDirectOCIRestore(s, task, store))
	require.Zero(t, store.downloads)
	info, err := os.Lstat(*task.BaseModel.Spec.Storage.Path)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestDirectOCIRestoreRequiresFinalIntegrity(t *testing.T) {
	for _, failure := range []string{"invalid bytes", "verification error"} {
		t.Run(failure, func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			path := filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights")
			require.NoError(t, os.Remove(path))
			store.download = func(context.Context, []ociobjectstore.ObjectURI, string) error {
				return os.WriteFile(path, []byte("wrong"), 0o600)
			}
			validate := store.validate
			store.validate = func(uri ociobjectstore.ObjectURI, path string) (bool, error) {
				if store.downloads > 0 && failure == "verification error" {
					return false, errors.New("remote metadata unavailable")
				}
				return validate(uri, path)
			}
			require.Error(t, runDirectOCIRestore(s, task, store))
			require.Equal(t, 1, store.downloads)
			require.Error(t, s.validateArtifactRestoreReport(context.Background(), task))
		})
	}
}

func TestDirectOCIRestoreRepairReferences(t *testing.T) {
	for _, reference := range []string{"direct", "local", "Shared", "during withdrawal", "receipt during withdrawal"} {
		t.Run(reference, func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			path := *task.BaseModel.Spec.Storage.Path
			require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("wrong"), 0o600))
			insert := func() {
				if reference == "Shared" {
					require.NoError(t, os.Symlink(path, filepath.Join(s.modelRootDir, "borrower")))
					return
				}
				if reference == "receipt during withdrawal" {
					client := s.kubeClient.(*fake.Clientset)
					object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), s.configMapReconciler.namespace, s.configMapReconciler.nodeName)
					require.NoError(t, err)
					cm := object.(*corev1.ConfigMap)
					key := getModelID(task.BaseModel, nil)
					entry, err := existingModelEntry(cm.Data, key)
					require.NoError(t, err)
					entry.DirectArtifactPendingDeletion = &DirectArtifactPendingDeletion{ModelUID: task.BaseModel.UID, Path: path}
					_, err = writeModelEntry(cm.Data, key, entry)
					require.NoError(t, err)
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					return
				}
				borrower := task.BaseModel.DeepCopy()
				borrower.Name, borrower.UID, borrower.Annotations = "borrower", "borrower-uid", nil
				if reference == "local" {
					borrower.Spec.Storage.StorageUri, borrower.Spec.Storage.Path = ptr("local://"+path), nil
				}
				_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(context.Background(), borrower, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			if strings.Contains(reference, "withdrawal") {
				scanned, injected := false, false
				s.omeClient.(*omefake.Clientset).PrependReactor("list", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) { scanned = true; return false, nil, nil })
				s.kubeClient.(*fake.Clientset).PrependReactor("get", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
					if scanned && !injected {
						injected = true
						insert()
					}
					return false, nil, nil
				})
			} else {
				insert()
			}
			require.Error(t, runDirectOCIRestore(s, task, store))
			require.Zero(t, store.downloads)
			b, err := os.ReadFile(filepath.Join(path, "weights"))
			require.NoError(t, err)
			require.Equal(t, "wrong", string(b))
		})
	}
}

func TestDirectOCIRestoreCancellationRetainsWriterLock(t *testing.T) {
	s, task, store := newDirectOCIRestoreFixture(t)
	path := *task.BaseModel.Spec.Storage.Path
	require.NoError(t, os.Remove(filepath.Join(path, "weights")))
	started, release := make(chan context.Context, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	download := store.download
	store.download = func(ctx context.Context, uris []ociobjectstore.ObjectURI, path string) error {
		started <- ctx
		<-release
		return download(ctx, uris, path)
	}
	done := make(chan error, 1)
	go func() { done <- runDirectOCIRestore(s, task, store) }()
	writerCtx := <-started
	current := task.BaseModel.DeepCopy()
	current.Annotations = map[string]string{artifactResidencyAnnotation: "Evicted"}
	_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
	require.NoError(t, err)
	eviction := &GopherTask{TaskType: Evict, BaseModel: current}
	require.NoError(t, s.processTask(eviction))
	require.ErrorIs(t, writerCtx.Err(), context.Canceled)
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: s.modelRootDir, ChildModelPath: path})
	require.NoError(t, err)
	if acquired {
		_ = lock.Close()
	}
	require.False(t, acquired)
	unblock()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Same(t, eviction, receiveDirectRetry(t, s))
	require.NoError(t, s.processTask(eviction))
	require.NoDirExists(t, path)
}

func TestDirectOCIRestoreRechecksAuthority(t *testing.T) {
	for _, test := range []struct{ stage, change string }{
		{"list", "request"}, {"inspection", "source"}, {"transfer", "model"},
		{"transfer", "node"}, {"transfer", "path"}, {"transfer", "placement"},
		{"report", "request"}, {"labels", "evicted"},
	} {
		t.Run(test.stage+"/"+test.change, func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			injected := false
			mutate := func() {
				if !injected {
					injected = true
					changeDirectHfRestoreAuthority(t, s, task, test.change)
				}
			}
			switch test.stage {
			case "list":
				list := store.list
				store.list = func(uri ociobjectstore.ObjectURI) ([]objectstorage.ObjectSummary, error) { mutate(); return list(uri) }
			case "inspection":
				validate := store.validate
				store.validate = func(uri ociobjectstore.ObjectURI, path string) (bool, error) { mutate(); return validate(uri, path) }
			case "transfer":
				require.NoError(t, os.Remove(filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights")))
				download := store.download
				store.download = func(ctx context.Context, uris []ociobjectstore.ObjectURI, path string) error {
					err := download(ctx, uris, path)
					mutate()
					return err
				}
			case "report":
				s.kubeClient.(*fake.Clientset).PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
					require.NoError(t, err)
					if entry.ArtifactRehydrationID == "" {
						return false, nil, nil
					}
					mutate()
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, errors.New("conflict"))
				})
			case "labels":
				s.nodeLabelReconciler.opRetry = 2
				s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
					if !strings.Contains(string(action.(ktesting.PatchAction).GetPatch()), "request-1") {
						return false, nil, nil
					}
					mutate()
					return true, nil, errors.New("retry labels")
				})
			}
			require.Error(t, runDirectOCIRestore(s, task, store))
			require.True(t, injected)
			node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Empty(t, node.Labels[testRequestLabel(task)])
		})
	}
}

func TestDirectOCIRestoreLostPublicationRecovers(t *testing.T) {
	for _, stage := range []string{"report", "labels", "missing entry"} {
		t.Run(stage, func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			client := s.kubeClient.(*fake.Clientset)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if injected || stage != "report" {
					return false, nil, nil
				}
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
				require.NoError(t, err)
				if entry.ArtifactRehydrationID == "" {
					return false, nil, nil
				}
				lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: s.modelRootDir, ChildModelPath: *task.BaseModel.Spec.Storage.Path})
				require.NoError(t, err)
				if acquired {
					_ = lock.Close()
				}
				require.False(t, acquired)
				injected = true
				require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				return true, nil, errors.New("lost report response")
			})
			client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				if !strings.Contains(string(action.(ktesting.PatchAction).GetPatch()), "request-1") {
					return false, nil, nil
				}
				object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), s.configMapReconciler.namespace, s.configMapReconciler.nodeName)
				require.NoError(t, err)
				entry, err := existingModelEntry(object.(*corev1.ConfigMap).Data, getModelID(task.BaseModel, nil))
				require.NoError(t, err)
				require.Equal(t, "request-1", entry.ArtifactRehydrationID)
				if injected || stage != "labels" {
					return false, nil, nil
				}
				injected = true
				return true, nil, errors.New("unavailable labels")
			})
			if stage == "missing entry" {
				require.NoError(t, runDirectOCIRestore(s, task, store))
				cm, err := s.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				delete(cm.Data, getModelID(task.BaseModel, nil))
				_, err = client.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			} else {
				require.Error(t, runDirectOCIRestore(s, task, store))
				require.True(t, injected)
			}
			s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, client, s.logger)
			WithArtifactEviction(s.omeClient, s.artifactNodeUID)(s)
			s.hfArtifactHandlerOnce, s.taskQueue = sync.Once{}, newGopherTaskQueue()
			s.sharedHfArtifactHandler()
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			recovery := popArtifactRecovery(t, s)
			require.NoError(t, runDirectOCIRestore(s, recovery, store))
			require.Zero(t, store.downloads)
			require.NoError(t, s.validateArtifactRestoreReport(context.Background(), task))
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			require.Zero(t, s.taskQueue.len())
			require.NoError(t, runDirectOCIRestore(s, recovery, store))
		})
	}
}

func TestDirectOCIRestorePendingOldPathAndPolicy(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "reuse"}[reuse], func(t *testing.T) {
			s, task, store := newDirectOCIRestoreFixture(t)
			oldPath, err := s.directEvictionPath(*task.BaseModel.Spec.Storage.Path)
			require.NoError(t, err)
			require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
				key := getModelID(task.BaseModel, nil)
				entry, err := existingModelEntry(cm.Data, key)
				require.NoError(t, err)
				entry.ModelUID, entry.NodeUID, entry.Status = task.BaseModel.UID, s.artifactNodeUID, ModelStatusUpdating
				entry.DirectArtifactPendingDeletion = &DirectArtifactPendingDeletion{ModelUID: task.BaseModel.UID, Path: oldPath}
				return writeModelEntry(cm.Data, key, entry)
			}))
			task.BaseModel.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "new-destination"))
			if reuse {
				policy := v1beta1.ReuseIfExists
				task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			}
			_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			list := store.list
			store.list = func(uri ociobjectstore.ObjectURI) ([]objectstorage.ObjectSummary, error) {
				require.NoDirExists(t, oldPath)
				return list(uri)
			}
			require.NoError(t, runDirectOCIRestore(s, task, store))
			info, err := os.Lstat(*task.BaseModel.Spec.Storage.Path)
			require.NoError(t, err)
			require.True(t, info.IsDir())
			require.Equal(t, 1, store.downloads)
		})
	}
}

func TestDirectOCIRestoreRecoveryPreservesExplicitOverride(t *testing.T) {
	s, task, store := newDirectOCIRestoreFixture(t)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Equal(t, 1, s.taskQueue.len())
	override := &GopherTask{TaskType: DownloadOverride, BaseModel: task.BaseModel.DeepCopy()}
	s.enqueueTask(override)
	recovery, ok := s.taskQueue.popNormal()
	require.True(t, ok)
	require.NoError(t, runDirectOCIRestore(s, recovery, nil))
	queued, ok := s.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, override, queued)
	require.NoError(t, runDirectOCIRestore(s, queued, store))
	require.Zero(t, store.downloads)
}

func TestOrdinaryOCIDownloadWithoutRestoreIsUnchanged(t *testing.T) {
	s, task, store := newDirectOCIRestoreFixture(t)
	task.BaseModel.Annotations = nil
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, runDirectOCIRestore(s, task, store))
	require.Equal(t, 1, store.downloads)
	cm, err := s.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
	require.NoError(t, err)
	require.Equal(t, ModelStatusReady, entry.Status)
	require.Empty(t, entry.ArtifactRehydrationID)
}

func TestDirectOCIRestoreAdmitsOrdinarySource(t *testing.T) {
	s, task := newDirectEvictionFixture(t, "oci://n/ns/b/bucket/o/model")
	task.TaskType = Download
	task.BaseModel.Annotations = map[string]string{testRestoreAnnotation: "request-1"}
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	expected := errors.New("ordinary OCI source reached")
	err = s.processTaskWithSourceAdapters(task, true, nil, func(context.Context, *ociobjectstore.ObjectURI, string, *GopherTask) error {
		return expected
	})
	require.ErrorContains(t, err, expected.Error())
}
