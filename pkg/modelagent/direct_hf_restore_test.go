package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

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
	"sigs.k8s.io/ome/pkg/xet"
)

func newDirectHfRestoreFixture(t *testing.T) (*Gopher, *GopherTask, directHfSource, *int) {
	t.Helper()
	s, task := newDirectEvictionFixture(t, "hf://org/model")
	s.taskQueue = newGopherTaskQueue()
	task.TaskType = Download
	task.BaseModel.Annotations = map[string]string{testRestoreAnnotation: "request-1"}
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	manifest := testHfSnapshotManifest(map[string]string{"weights": "model"})
	manifest.SHA = strings.Repeat("a", 40)
	downloads := new(int)
	source := directHfSource{
		resolve: func(context.Context, string, string, string, string) (string, error) { return manifest.SHA, nil },
		manifest: func(context.Context, string, string, string, string) (hfSnapshotManifest, error) {
			return manifest, nil
		},
		download: func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
			*downloads++
			require.Equal(t, manifest.SHA, config.Revision)
			require.NoError(t, os.MkdirAll(config.LocalDir, 0o755))
			path := filepath.Join(config.LocalDir, "weights")
			// Match Xet's same-size shortcut: repair must remove corrupt bytes.
			if info, err := os.Stat(path); err == nil && info.Size() == 5 {
				return nil
			}
			return os.WriteFile(path, []byte("model"), 0o600)
		},
	}
	return s, task, source, downloads
}

func runDirectHfRestore(s *Gopher, task *GopherTask, source directHfSource) error {
	return s.processTaskWithSourceAdapters(task, true, func(ctx context.Context, task *GopherTask, spec v1beta1.BaseModelSpec, allow bool) (bool, error) {
		return source.process(ctx, s, task, spec, allow)
	}, nil)
}

func TestDirectHfRestoreValidatesConfiguredDirectory(t *testing.T) {
	for _, state := range []string{"healthy", "missing", "incomplete", "corrupt"} {
		t.Run(state, func(t *testing.T) {
			s, task, source, downloads := newDirectHfRestoreFixture(t)
			path := *task.BaseModel.Spec.Storage.Path
			switch state {
			case "missing":
				require.NoError(t, os.RemoveAll(path))
			case "incomplete":
				require.NoError(t, os.Remove(filepath.Join(path, "weights")))
			case "corrupt":
				require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("wrong"), 0o600))
			}
			require.NoError(t, runDirectHfRestore(s, task, source))
			if state == "healthy" {
				require.Zero(t, *downloads)
			} else {
				require.Equal(t, 1, *downloads)
			}
			contents, err := os.ReadFile(filepath.Join(path, "weights"))
			require.NoError(t, err)
			require.Equal(t, "model", string(contents))
			info, err := os.Lstat(path)
			require.NoError(t, err)
			require.True(t, info.IsDir())
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

func TestDirectHfRestoreInspectionFailurePreservesBytes(t *testing.T) {
	for _, failure := range []string{"resolve", "manifest", "revision", "file inspection"} {
		t.Run(failure, func(t *testing.T) {
			s, task, source, downloads := newDirectHfRestoreFixture(t)
			path := filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights")
			require.NoError(t, os.WriteFile(path, []byte("wrong"), 0o600))
			switch failure {
			case "resolve":
				source.resolve = func(context.Context, string, string, string, string) (string, error) {
					return "", errors.New("resolve unavailable")
				}
			case "manifest":
				source.manifest = func(context.Context, string, string, string, string) (hfSnapshotManifest, error) {
					return hfSnapshotManifest{}, errors.New("manifest unavailable")
				}
			case "revision":
				source.manifest = func(context.Context, string, string, string, string) (hfSnapshotManifest, error) {
					return testHfSnapshotManifest(map[string]string{"weights": "model"}), nil
				}
			case "file inspection":
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Mkdir(path, 0o755))
			}
			require.Error(t, runDirectHfRestore(s, task, source))
			require.Zero(t, *downloads)
			if failure == "file inspection" {
				require.DirExists(t, path)
			} else {
				contents, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, "wrong", string(contents))
			}
		})
	}
}

func TestDirectHfRestoreResidentReusePolicyDoesNotMigrate(t *testing.T) {
	s, task, source, downloads := newDirectHfRestoreFixture(t)
	policy := v1beta1.ReuseIfExists
	task.BaseModel.Spec.Storage.DownloadPolicy = &policy
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, runDirectHfRestore(s, task, source))
	require.Zero(t, *downloads)
	info, err := os.Lstat(*task.BaseModel.Spec.Storage.Path)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestDirectHfRestorePeriodicRecovery(t *testing.T) {
	s, task, source, downloads := newDirectHfRestoreFixture(t)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.NoError(t, runDirectHfRestore(s, popArtifactRecovery(t, s), source))
	require.Zero(t, *downloads)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Zero(t, s.taskQueue.len())
	require.NoError(t, runDirectHfRestore(s, task, source))
}

func changeDirectHfRestoreAuthority(t *testing.T, s *Gopher, task *GopherTask, change string) {
	t.Helper()
	current := task.BaseModel.DeepCopy()
	switch change {
	case "request":
		current.Annotations[testRestoreAnnotation] = "request-2"
	case "model":
		current.UID = "replacement"
	case "source":
		current.Spec.Storage.StorageUri = ptr("hf://org/other")
	case "path":
		current.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "other"))
	case "placement":
		current.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
	case "evicted":
		current.Annotations[artifactResidencyAnnotation] = "Evicted"
	case "deleted":
		require.NoError(t, s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Delete(context.Background(), current.Name, metav1.DeleteOptions{}))
		return
	case "node":
		client := s.kubeClient.(*fake.Clientset)
		object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
		require.NoError(t, err)
		node := object.(*corev1.Node)
		node.UID = "replacement"
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
	}
	_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func TestDirectHfRestoreRechecksAuthority(t *testing.T) {
	for _, stage := range []string{"manifest", "download", "report", "labels"} {
		for _, change := range []string{"request", "model", "node", "source", "path", "placement", "evicted", "deleted"} {
			t.Run(stage+"/"+change, func(t *testing.T) {
				s, task, source, _ := newDirectHfRestoreFixture(t)
				injected := false
				mutate := func() {
					if !injected {
						injected = true
						changeDirectHfRestoreAuthority(t, s, task, change)
					}
				}
				switch stage {
				case "manifest":
					fetch := source.manifest
					source.manifest = func(ctx context.Context, repo, revision, token, endpoint string) (hfSnapshotManifest, error) {
						mutate()
						return fetch(ctx, repo, revision, token, endpoint)
					}
				case "download":
					require.NoError(t, os.Remove(filepath.Join(*task.BaseModel.Spec.Storage.Path, "weights")))
					download := source.download
					source.download = func(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
						err := download(ctx, task, config)
						mutate()
						return err
					}
				case "report":
					s.kubeClient.(*fake.Clientset).PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
						cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
						entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
						require.NoError(t, err)
						if entry.Status != ModelStatusReady || entry.ArtifactRehydrationID == "" {
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
				require.Error(t, runDirectHfRestore(s, task, source))
				require.True(t, injected)
				node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				require.Empty(t, node.Labels[testRequestLabel(task)])
			})
		}
	}
}

func TestDirectHfRestorePublicationFailuresRecoverAfterRestart(t *testing.T) {
	for _, stage := range []string{"report", "lost report", "labels", "lost labels"} {
		t.Run(stage, func(t *testing.T) {
			s, task, source, downloads := newDirectHfRestoreFixture(t)
			client := s.kubeClient.(*fake.Clientset)
			injected := false
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if injected || !strings.Contains(stage, "report") {
					return false, nil, nil
				}
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
				require.NoError(t, err)
				if entry.Status != ModelStatusReady || entry.ArtifactRehydrationID == "" {
					return false, nil, nil
				}
				lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: s.modelRootDir, ChildModelPath: *task.BaseModel.Spec.Storage.Path})
				require.NoError(t, err)
				if acquired {
					_ = lock.Close()
				}
				require.False(t, acquired, "Direct validation lock must survive report publication")
				injected = true
				if stage == "lost report" {
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				}
				return true, nil, errors.New("report response unavailable")
			})
			client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				if !strings.Contains(string(action.(ktesting.PatchAction).GetPatch()), "request-1") {
					return false, nil, nil
				}
				object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), s.configMapReconciler.namespace, s.configMapReconciler.nodeName)
				require.NoError(t, err)
				entry, err := existingModelEntry(object.(*corev1.ConfigMap).Data, getModelID(task.BaseModel, nil))
				require.NoError(t, err)
				require.Equal(t, "request-1", entry.ArtifactRehydrationID, "ConfigMap proof precedes paired labels")
				if injected || !strings.Contains(stage, "labels") {
					return false, nil, nil
				}
				injected = true
				if stage == "lost labels" {
					var patch []map[string]interface{}
					require.NoError(t, json.Unmarshal(action.(ktesting.PatchAction).GetPatch(), &patch))
					require.Equal(t, "/metadata/uid", patch[0]["path"])
					require.Equal(t, "/metadata/resourceVersion", patch[1]["path"])
					object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := object.(*corev1.Node)
					key, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
					require.NoError(t, err)
					node.Labels[key], node.Labels[testRequestLabel(task)] = string(Ready), "request-1"
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
				}
				return true, nil, errors.New("label response unavailable")
			})
			require.Error(t, runDirectHfRestore(s, task, source))
			require.True(t, injected)
			s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.kubeClient, s.logger)
			WithArtifactEviction(s.omeClient, s.artifactNodeUID)(s)
			s.hfArtifactHandlerOnce, s.taskQueue = sync.Once{}, newGopherTaskQueue()
			s.sharedHfArtifactHandler()
			validations := 0
			fetch := source.manifest
			source.manifest = func(ctx context.Context, repo, revision, token, endpoint string) (hfSnapshotManifest, error) {
				validations++
				return fetch(ctx, repo, revision, token, endpoint)
			}
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			require.NoError(t, runDirectHfRestore(s, popArtifactRecovery(t, s), source))
			require.Equal(t, 1, validations)
			require.Zero(t, *downloads)
			require.NoError(t, s.validateArtifactRestoreReport(context.Background(), task))
		})
	}
}

func TestDirectHfRestoreBorrowersAndRepairSafety(t *testing.T) {
	for _, reference := range []string{"direct", "local", "recorded child", "unrecorded link", "recorded borrower"} {
		for _, healthy := range []bool{true, false} {
			t.Run(reference+"/healthy="+map[bool]string{true: "yes", false: "no"}[healthy], func(t *testing.T) {
				s, task, source, downloads := newDirectHfRestoreFixture(t)
				path, ctx := *task.BaseModel.Spec.Storage.Path, context.Background()
				if !healthy {
					require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("wrong"), 0o600))
				}
				switch reference {
				case "direct", "local":
					borrower := task.BaseModel.DeepCopy()
					borrower.Name, borrower.UID, borrower.Annotations = "borrower", "borrower-uid", nil
					if reference == "local" {
						borrower.Spec.Storage.StorageUri, borrower.Spec.Storage.Path = ptr("local://"+path), nil
					}
					_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(ctx, borrower, metav1.CreateOptions{})
					require.NoError(t, err)
				case "recorded child":
					require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
						key := getModelID(task.BaseModel, nil)
						entry, err := existingModelEntry(cm.Data, key)
						require.NoError(t, err)
						entry.Config.Artifact.ChildrenPaths = []string{filepath.Join(s.modelRootDir, "borrower")}
						return writeModelEntry(cm.Data, key, entry)
					}))
				case "unrecorded link":
					require.NoError(t, os.Symlink(path, filepath.Join(s.modelRootDir, "borrower")))
				case "recorded borrower":
					require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
						return writeModelEntry(cm.Data, "borrower", ModelEntry{Name: "borrower", Config: &ModelConfig{Artifact: Artifact{ParentPath: map[string]string{"owner": path}}}})
					}))
				}
				err := runDirectHfRestore(s, task, source)
				if healthy {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				require.Zero(t, *downloads)
				contents, err := os.ReadFile(filepath.Join(path, "weights"))
				require.NoError(t, err)
				if healthy {
					require.Equal(t, "model", string(contents))
				} else {
					require.Equal(t, "wrong", string(contents))
				}
			})
		}
	}
}

func TestDirectHfRestoreRepairRequiresStrictWithdrawalAndReferences(t *testing.T) {
	for _, failure := range []string{"withdrawal", "lookup", "borrower during withdrawal", "receipt during withdrawal", "Shared ownership during withdrawal"} {
		t.Run(failure, func(t *testing.T) {
			s, task, source, downloads := newDirectHfRestoreFixture(t)
			path := *task.BaseModel.Spec.Storage.Path
			require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("wrong"), 0o600))
			if failure == "lookup" {
				s.omeClient.(*omefake.Clientset).PrependReactor("list", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("unavailable references")
				})
			} else if failure == "withdrawal" {
				s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("unavailable withdrawal")
				})
			} else {
				scanned, injected := false, false
				s.omeClient.(*omefake.Clientset).PrependReactor("list", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) { scanned = true; return false, nil, nil })
				s.kubeClient.(*fake.Clientset).PrependReactor("get", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
					// Inject after the initial reference scan, in repair's strict
					// withdrawal checks, even if Updating is already idempotent.
					if !scanned || injected {
						return false, nil, nil
					}
					injected = true
					if failure == "borrower during withdrawal" {
						borrower := task.BaseModel.DeepCopy()
						borrower.Name, borrower.UID = "borrower", "borrower-uid"
						_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(context.Background(), borrower, metav1.CreateOptions{})
						require.NoError(t, err)
					} else {
						client := s.kubeClient.(*fake.Clientset)
						object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), s.configMapReconciler.namespace, s.configMapReconciler.nodeName)
						require.NoError(t, err)
						cm := object.(*corev1.ConfigMap)
						key := getModelID(task.BaseModel, nil)
						entry, err := existingModelEntry(cm.Data, key)
						require.NoError(t, err)
						if failure == "receipt during withdrawal" {
							entry.DirectArtifactPendingDeletion = &DirectArtifactPendingDeletion{ModelUID: task.BaseModel.UID, Path: path}
						} else {
							entry.HfArtifactKey = "another-parent"
						}
						_, err = writeModelEntry(cm.Data, key, entry)
						require.NoError(t, err)
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					}
					return false, nil, nil
				})
			}
			require.Error(t, runDirectHfRestore(s, task, source))
			require.Zero(t, *downloads)
			contents, err := os.ReadFile(filepath.Join(path, "weights"))
			require.NoError(t, err)
			require.Equal(t, "wrong", string(contents))
		})
	}
}

func TestDirectHfRestoreCleanupAndLayoutAfterRestart(t *testing.T) {
	for _, stage := range []string{"completed", "pending", "pending old path"} {
		for _, reuse := range []bool{false, true} {
			t.Run(stage+"/reuse="+map[bool]string{true: "yes", false: "no"}[reuse], func(t *testing.T) {
				s, task, source, downloads := newDirectHfRestoreFixture(t)
				ctx, oldPath := context.Background(), *task.BaseModel.Spec.Storage.Path
				if stage == "completed" {
					task.TaskType = Evict
					task.BaseModel.Annotations = map[string]string{artifactResidencyAnnotation: "Evicted"}
					_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
					require.NoError(t, err)
					require.NoError(t, s.processTask(task))
				} else {
					seedDirectCleanup(t, s, task)
				}
				if stage == "pending old path" {
					task.BaseModel.Spec.Storage.Path = ptr(filepath.Join(s.modelRootDir, "new-destination"))
				}
				if reuse {
					policy := v1beta1.ReuseIfExists
					task.BaseModel.Spec.Storage.DownloadPolicy = &policy
				}
				task.BaseModel.Annotations = map[string]string{testRestoreAnnotation: "request-1"}
				_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
				require.NoError(t, err)
				s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.kubeClient, s.logger)
				WithArtifactEviction(s.omeClient, s.artifactNodeUID)(s)
				s.hfArtifactHandlerOnce, s.artifactRouting = sync.Once{}, gopherArtifactRouting{}
				s.sharedHfArtifactHandler()
				task = &GopherTask{TaskType: Download, BaseModel: task.BaseModel}
				resolve := source.resolve
				source.resolve = func(ctx context.Context, repo, revision, token, endpoint string) (string, error) {
					require.NoDirExists(t, oldPath, "settle old path before resolving new source")
					cm, err := s.configMapReconciler.getConfigMap(ctx)
					require.NoError(t, err)
					entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
					require.NoError(t, err)
					require.Nil(t, entry.DirectArtifactPendingDeletion)
					return resolve(ctx, repo, revision, token, endpoint)
				}
				require.NoError(t, runDirectHfRestore(s, task, source))
				if stage == "pending" && reuse {
					// Completing cleanup retains the old child lock until this
					// attempt returns. The normal Shared retry then acquires it.
					require.Zero(t, *downloads)
					retry := receiveDirectRetry(t, s)
					require.Same(t, task, retry)
					require.NoError(t, runDirectHfRestore(s, retry, source))
				}
				require.Equal(t, 1, *downloads)
				info, err := os.Lstat(*task.BaseModel.Spec.Storage.Path)
				require.NoError(t, err)
				require.Equal(t, reuse, info.Mode()&os.ModeSymlink != 0)
				cm, err := s.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
				require.NoError(t, err)
				require.Equal(t, "request-1", entry.ArtifactRehydrationID)
				require.Nil(t, entry.DirectArtifactPendingDeletion)
				require.NotContains(t, cm.Data[getModelID(task.BaseModel, nil)], "artifactLayout")
			})
		}
	}
}

func TestDirectHfRestoreRecoveryRevalidatesMissingReports(t *testing.T) {
	for _, missing := range []string{"entry", "report", "request label", "Ready label"} {
		t.Run(missing, func(t *testing.T) {
			s, task, source, downloads := newDirectHfRestoreFixture(t)
			// A resident directory remains Direct even when its entry is lost.
			policy := v1beta1.ReuseIfExists
			task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, runDirectHfRestore(s, task, source))
			if strings.Contains(missing, "label") {
				node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				key := testRequestLabel(task)
				if missing == "Ready label" {
					key, err = getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
					require.NoError(t, err)
				}
				delete(node.Labels, key)
				_, err = s.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
				require.NoError(t, err)
			} else {
				cm, err := s.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				key := getModelID(task.BaseModel, nil)
				if missing == "entry" {
					delete(cm.Data, key)
				} else {
					entry, err := existingModelEntry(cm.Data, key)
					require.NoError(t, err)
					entry.ArtifactRehydrationID = ""
					_, err = writeModelEntry(cm.Data, key, entry)
					require.NoError(t, err)
				}
				_, err = s.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			validations := 0
			fetch := source.manifest
			source.manifest = func(ctx context.Context, repo, revision, token, endpoint string) (hfSnapshotManifest, error) {
				validations++
				return fetch(ctx, repo, revision, token, endpoint)
			}
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			recovery := popArtifactRecovery(t, s)
			require.NoError(t, runDirectHfRestore(s, recovery, source))
			require.Equal(t, 1, validations)
			require.Zero(t, *downloads)
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			require.Zero(t, s.taskQueue.len())
			// An already-satisfied queued recovery does not enter the source.
			require.NoError(t, runDirectHfRestore(s, recovery, source))
			require.Equal(t, 1, validations)
		})
	}
}

func TestDirectHfRestoreRecoveryPreservesExplicitOverride(t *testing.T) {
	s, task, source, downloads := newDirectHfRestoreFixture(t)
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.NoError(t, s.recoverArtifactReports(context.Background()))
	require.Equal(t, 1, s.taskQueue.len())
	override := &GopherTask{TaskType: DownloadOverride, BaseModel: task.BaseModel.DeepCopy()}
	s.enqueueTask(override)
	require.Equal(t, 2, s.taskQueue.len())
	recovery, ok := s.taskQueue.popNormal()
	require.True(t, ok)
	require.True(t, recovery.ArtifactReportRecovery)
	require.NoError(t, runDirectHfRestore(s, recovery, directHfSource{}))
	queued, ok := s.taskQueue.popNormal()
	require.True(t, ok)
	require.Same(t, override, queued)
	validations := 0
	fetch := source.manifest
	source.manifest = func(ctx context.Context, repo, revision, token, endpoint string) (hfSnapshotManifest, error) {
		validations++
		return fetch(ctx, repo, revision, token, endpoint)
	}
	require.NoError(t, runDirectHfRestore(s, queued, source))
	require.Equal(t, 1, validations)
	require.Zero(t, *downloads)
}

func TestDirectHfRestoreExcludesUnsupportedSourcesAndPlacement(t *testing.T) {
	for _, change := range []string{"local", "shape", "placement", "outside", "symlink"} {
		t.Run(change, func(t *testing.T) {
			s, task, source, downloads := newDirectHfRestoreFixture(t)
			switch change {
			case "local":
				task.BaseModel.Spec.Storage.StorageUri = ptr("local://" + *task.BaseModel.Spec.Storage.Path)
			case "shape":
				task.BaseModel.Spec.ModelFormat.Name = constants.TensorRTLLM
			case "placement":
				task.BaseModel.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
			case "outside":
				task.BaseModel.Spec.Storage.Path = ptr(t.TempDir())
			case "symlink":
				path := filepath.Join(s.modelRootDir, "alias")
				require.NoError(t, os.Symlink(*task.BaseModel.Spec.Storage.Path, path))
				task.BaseModel.Spec.Storage.Path = &path
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, s.recoverArtifactReports(context.Background()))
			require.Zero(t, s.taskQueue.len())
			require.Error(t, runDirectHfRestore(s, task, source))
			require.Zero(t, *downloads)
		})
	}
}

func TestDirectHfRestoreCancellationRetainsWriterLock(t *testing.T) {
	s, task, source, _ := newDirectHfRestoreFixture(t)
	path := *task.BaseModel.Spec.Storage.Path
	require.NoError(t, os.Remove(filepath.Join(path, "weights")))
	started, release := make(chan context.Context, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	download := source.download
	source.download = func(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
		started <- ctx
		<-release // A transfer may finish its write after cancellation.
		return download(ctx, task, config)
	}
	done := make(chan error, 1)
	go func() { done <- runDirectHfRestore(s, task, source) }()
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
	node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, node.Labels[testRequestLabel(task)])
}
