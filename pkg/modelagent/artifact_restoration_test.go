package modelagent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/xet"
)

func TestRestorationCapturedReceiptCannotDeleteRestoredBytes(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
		require.NoError(t, err)
		entry.DirectArtifactPath = path
		return writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
	}))
	// Another process finished the captured receipt and restored the same UID
	// and request before this process acquired the path lock. Ready ownership
	// alone does not authorize replaying that old receipt.
	receipt := DirectArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path}
	require.Error(t, g.settleDirectRestorationReceipt(context.Background(), task, receipt))
	require.DirExists(t, path)
	require.Equal(t, ModelStatusReady, directArtifactEntry(t, g, task).Status)
}

func TestRestorationReceiptRechecksSafetyAfterWithdrawal(t *testing.T) {
	for _, change := range []string{"local borrower", "completed receipt"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newEvictionTestModel(t)
			require.NoError(t, g.persistDirectEviction(ctx, task, path, false))
			receipt := *directArtifactEntry(t, g, task).DirectArtifactPendingEviction
			task.TaskType = Download
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			changed := false
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
				if !changed {
					changed = true
					if change == "local borrower" {
						borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "borrower", Namespace: "default", UID: "borrower"},
							Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
						require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(borrower))
					} else {
						tracker := g.kubeClient.(*k8sfake.Clientset).Tracker()
						resource := corev1.SchemeGroupVersion.WithResource("configmaps")
						obj, err := tracker.Get(resource, g.configMapReconciler.namespace, g.configMapReconciler.nodeName)
						require.NoError(t, err)
						cm := obj.(*corev1.ConfigMap)
						entry, err := existingModelEntry(cm.Data, getModelID(task.BaseModel, nil))
						require.NoError(t, err)
						entry.DirectArtifactPendingEviction = nil
						entry.Status, entry.DirectArtifactPath = ModelStatusReady, path
						_, err = writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), entry)
						require.NoError(t, err)
						require.NoError(t, tracker.Update(resource, cm, cm.Namespace))
					}
				}
				return false, nil, nil
			})
			require.Error(t, g.settleDirectRestorationReceipt(ctx, task, receipt))
			require.True(t, changed)
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestRestorationMetadataRejectsSupersededRequest(t *testing.T) {
	for _, duringCAS := range []bool{false, true} {
		t.Run(map[bool]string{false: "before parse", true: "inside mutation"}[duringCAS], func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "old"}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			g.modelConfigParser = modelparser.NewModelConfigParser(g.modelClient, g.logger)
			config, err := os.ReadFile("../modelparser/testdata/tiny-random-PhiModel/config.json")
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(path, "config.json"), config, 0o600))
			before := directArtifactEntry(t, g, task).Config
			latest := task.BaseModel.DeepCopy()
			latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new"
			latest.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "new"))
			advance := func() {
				require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
			}
			if duringCAS {
				g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					advance()
					return false, nil, nil
				})
			} else {
				advance()
			}
			err = g.safeParseAndUpdateModelConfig(ctx, path, task.BaseModel, nil, &Artifact{Sha: "stale", ParentPath: map[string]string{"stale": path}})
			require.Error(t, err)
			require.Equal(t, before, directArtifactEntry(t, g, task).Config)
		})
	}
}

func TestRestorationSettlesOriginalDirectReceiptBeforeSource(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending receipt", true: "completed history"}[completed], func(t *testing.T) {
			ctx := context.Background()
			g, task, input, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "request-1"
			oldPath := filepath.Join(g.modelRootDir, "old-destination")
			oldPath, err := ownedArtifactPath(g.modelRootDir, oldPath)
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(oldPath, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(oldPath, "weights"), []byte("old"), 0o600))
			require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
				entry := ModelEntry{Name: task.BaseModel.Name, ModelUID: task.BaseModel.UID, Status: ModelStatusEvicted, DirectArtifactPath: oldPath}
				if !completed {
					entry.Status = ModelStatusEvicting
					entry.DirectArtifactPendingEviction = &DirectArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: oldPath}
				}
				return writeModelEntry(cm.Data, input.ChildModelKey, entry)
			}))
			// Restart with only durable receipts/history and current Model evidence.
			g.configMapReconciler = NewConfigMapReconciler(g.configMapReconciler.nodeName, g.configMapReconciler.namespace, g.kubeClient, g.logger)
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			_, err = g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
			require.NoError(t, err)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			resolve := source.resolve
			source.resolve = func(ctx context.Context, id, revision, token, endpoint string) (string, error) {
				entry := directArtifactEntry(t, g, task)
				require.Nil(t, entry.DirectArtifactPendingEviction)
				require.Empty(t, entry.DirectArtifactPath, "completed history must not become a renewed ownership claim")
				require.Equal(t, ModelStatusUpdating, entry.Status)
				if completed {
					require.FileExists(t, filepath.Join(oldPath, "weights"), "history must never delete reused bytes")
				} else {
					require.NoDirExists(t, oldPath)
				}
				return resolve(ctx, id, revision, token, endpoint)
			}
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			require.Equal(t, 1, *downloads)
		})
	}
}

func TestRestorationValidatesHfBytes(t *testing.T) {
	for _, shared := range []bool{false, true} {
		for _, state := range []string{"healthy", "corrupt", "borrowed healthy", "borrowed corrupt"} {
			t.Run(map[bool]string{false: "direct/", true: "shared/"}[shared]+state, func(t *testing.T) {
				ctx := context.Background()
				g, task, input, source, downloads := newTestDirectHfSource(t)
				if !shared {
					task.BaseModel.Spec.Storage.DownloadPolicy = nil
				}
				waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
				require.NoError(t, err)
				require.False(t, waiting)
				require.Equal(t, 1, *downloads)
				path := input.ChildModelPath
				if shared {
					path = input.Parent.LocalPath
				}
				corrupt := state == "corrupt" || state == "borrowed corrupt"
				if corrupt {
					require.NoError(t, os.WriteFile(filepath.Join(path, "model.safetensors"), []byte("CORRUPT"), 0o600))
				}
				task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "request-1"
				g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
				if state == "borrowed healthy" || state == "borrowed corrupt" {
					borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "borrower", Namespace: "default", UID: "borrower"},
						Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
					require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(borrower))
				}
				_, err = g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{
					constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name): string(Ready),
				}}}, metav1.CreateOptions{})
				require.NoError(t, err)
				g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
				waiting, err = source.process(ctx, g, task, task.BaseModel.Spec, true)
				if state == "borrowed corrupt" {
					require.True(t, waiting || err != nil, "repair must not destroy a borrower's bytes")
					require.Equal(t, 1, *downloads)
					contents, err := os.ReadFile(filepath.Join(path, "model.safetensors"))
					require.NoError(t, err)
					require.Equal(t, "CORRUPT", string(contents))
					return
				}
				require.NoError(t, err)
				require.False(t, waiting)
				wantDownloads := 1
				if corrupt {
					wantDownloads++
				}
				require.Equal(t, wantDownloads, *downloads)
				contents, err := os.ReadFile(filepath.Join(path, "model.safetensors"))
				require.NoError(t, err)
				require.Equal(t, "weights", string(contents))
			})
		}
	}
}

func TestRestorationStaleActualPathCannotWithdrawReady(t *testing.T) {
	for _, source := range []string{"fallback", "spaces", "local", "external"} {
		for _, oldRequest := range []string{"", "older"} {
			t.Run(source+"/"+oldRequest, func(t *testing.T) {
				g, task, path := newDirectArtifactTestModel(t)
				task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: oldRequest}
				switch source {
				case "fallback":
					task.BaseModel.Spec.Storage.Path = nil
				case "spaces":
					task.BaseModel.Spec.Storage.Path = stringPtr(path + " ")
				case "local":
					task.BaseModel.Spec.Storage.Path = nil
					task.BaseModel.Spec.Storage.StorageUri = stringPtr("local://" + path)
				case "external":
					task.BaseModel.Spec.Storage.Path = stringPtr(t.TempDir())
				}
				latest := task.BaseModel.DeepCopy()
				latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "newer"
				g.modelClient = omefake.NewSimpleClientset(latest)
				for _, state := range []ModelStateOnNode{Updating, Failed} {
					err := g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: state})
					require.Error(t, err)
					require.Equal(t, ModelStatusReady, directArtifactEntry(t, g, task).Status)
				}
			})
		}
	}
}

func TestRestorationRequestChangesDuringHfValidation(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "shared"}[shared], func(t *testing.T) {
			ctx := context.Background()
			g, task, _, source, downloads := newTestDirectHfSource(t)
			if !shared {
				task.BaseModel.Spec.Storage.DownloadPolicy = nil
			}
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "old"
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			manifest := source.manifest
			source.manifest = func(ctx context.Context, id, revision, token, endpoint string) (hfSnapshotManifest, error) {
				latest := task.BaseModel.DeepCopy()
				latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new"
				require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
				return manifest(ctx, id, revision, token, endpoint)
			}
			waiting, err = source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.True(t, waiting || err != nil, "a validation result cannot publish an obsolete request")
			require.Equal(t, 1, *downloads)
		})
	}
}

func TestRestorationSettlesOriginalSharedReceipt(t *testing.T) {
	for _, reference := range []string{"none", "child", "parent", "reused child", "lookup failure"} {
		t.Run(reference, func(t *testing.T) {
			ctx := context.Background()
			g, task, input, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore"
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			old := input
			old.Parent.Identity.CommitSHA = strings.Repeat("b", 40)
			old.Parent.Key = hfArtifactConfigMapKey(old.Parent.Identity)
			old.ChildModelPath = filepath.Join(input.ModelStoreRoot, "old-child")
			old.Parent.LocalPath = canonicalHfArtifactPath(old.ChildModelPath, old.Parent.Identity)
			handler := g.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(handler, old))
			_, err := handler.repository.removeModelReference(ctx, old.Parent, old.ChildModelKey, old.ChildModelUID, old.ChildModelPath, true)
			require.NoError(t, err)
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			_, err = g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
			require.NoError(t, err)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			if reference == "lookup failure" {
				g.modelClient.(*omefake.Clientset).PrependReactor("list", "basemodels", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("lookup offline") })
			}
			if reference == "reused child" {
				require.NoError(t, os.Remove(old.ChildModelPath))
				replacement := input
				replacement.ChildModelKey, replacement.ChildModelUID = "default.basemodel.replacement", "replacement"
				replacement.ChildModelPath = old.ChildModelPath
				seedTestChildModelEntry(t, handler.repository, replacement)
				require.NoError(t, runTestHfArtifactDownload(handler, replacement))
			}
			if reference == "parent" || reference == "child" {
				path := old.ChildModelPath
				if reference == "parent" {
					path = old.Parent.LocalPath
				}
				require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(&v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{Name: "borrower", Namespace: "default", UID: "borrower"},
					Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}},
				}))
			}
			resolves := 0
			resolve := source.resolve
			source.resolve = func(ctx context.Context, id, revision, token, endpoint string) (string, error) {
				resolves++
				pending, err := handler.repository.pendingDeletion(ctx, input.ChildModelKey)
				require.NoError(t, err)
				require.Nil(t, pending)
				if reference == "parent" || reference == "reused child" {
					require.DirExists(t, old.Parent.LocalPath)
				} else {
					require.NoDirExists(t, old.Parent.LocalPath)
				}
				if reference == "reused child" {
					assertChildSymlinkTarget(t, old.ChildModelPath, input.Parent.LocalPath)
				} else {
					assertChildPathMissing(t, old.ChildModelPath)
				}
				return resolve(ctx, id, revision, token, endpoint)
			}
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			if reference == "child" || reference == "lookup failure" {
				require.True(t, waiting || err != nil)
				require.Zero(t, resolves)
				require.Zero(t, *downloads)
				pending, err := handler.repository.pendingDeletion(ctx, input.ChildModelKey)
				require.NoError(t, err)
				require.NotNil(t, pending)
				require.DirExists(t, old.Parent.LocalPath)
				return
			}
			require.NoError(t, err)
			require.False(t, waiting)
			require.Equal(t, 1, resolves)
		})
	}
}

func TestRestorationStaleProgressDoesNotMutateReady(t *testing.T) {
	g, task, _ := newDirectArtifactTestModel(t)
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "old"}
	latest := task.BaseModel.DeepCopy()
	latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new"
	g.modelClient = omefake.NewSimpleClientset(latest)
	g.updateDirectHfProgress(context.Background(), task, &DownloadProgress{})
	require.Nil(t, directArtifactEntry(t, g, task).Progress)
}

func TestRestorationStaleNodePatchCannotWithdrawReady(t *testing.T) {
	g, task, _ := newDirectArtifactTestModel(t)
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "old"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	latest := task.BaseModel.DeepCopy()
	latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new"
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("get", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
		return false, nil, nil
	})
	require.Error(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Updating}))
	node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
}

func TestRestorationNodePatchFencesNewerReady(t *testing.T) {
	ctx := context.Background()
	g, task, _ := newDirectArtifactTestModel(t)
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "old"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.ResourceVersion = "1"
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	changed := false
	g.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
		if !changed {
			changed = true
			// Different destination: a newer process may publish Ready without
			// acquiring the old destination's family lock.
			latest := task.BaseModel.DeepCopy()
			latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new"
			latest.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "new"))
			require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
			node.ResourceVersion = "2"
			require.NoError(t, g.kubeClient.(*k8sfake.Clientset).Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
		}
		return false, nil, nil
	})
	require.Error(t, g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Updating}))
	require.True(t, changed)
	node, err = g.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
}

func TestRestorationDirectReceiptWithdrawsReadiness(t *testing.T) {
	for _, failPatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "lost eviction response", true: "patch failure"}[failPatch], func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore"}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			receipt := DirectArtifactPendingEviction{ModelUID: task.BaseModel.UID, Path: path}
			require.NoError(t, g.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
				return writeModelEntry(cm.Data, getModelID(task.BaseModel, nil), ModelEntry{Name: task.BaseModel.Name, ModelUID: task.BaseModel.UID, Status: ModelStatusEvicting, DirectArtifactPath: path, DirectArtifactPendingEviction: &receipt})
			}))
			if failPatch {
				g.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("offline") })
			}
			err := g.settleDirectRestorationReceipt(ctx, task, receipt)
			if failPatch {
				require.Error(t, err)
				require.FileExists(t, filepath.Join(path, "weights"))
				require.NotNil(t, directArtifactEntry(t, g, task).DirectArtifactPendingEviction)
			} else {
				require.NoError(t, err)
				require.NoDirExists(t, path)
				node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				require.NotEqual(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
			}
		})
	}
}

func TestRestorationPreservesActualHfDestination(t *testing.T) {
	for _, spelling := range []string{"nil", "empty", "spaces", "relative"} {
		t.Run(spelling, func(t *testing.T) {
			g, task, _, source, _ := newTestDirectHfSource(t)
			_, err := g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
			require.NoError(t, err)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore"
			switch spelling {
			case "nil":
				task.BaseModel.Spec.Storage.Path = nil
			case "empty":
				task.BaseModel.Spec.Storage.Path = stringPtr("")
			case "spaces":
				task.BaseModel.Spec.Storage.Path = stringPtr(*task.BaseModel.Spec.Storage.Path + " ")
			case "relative":
				cwd, err := os.Getwd()
				require.NoError(t, err)
				path, err := filepath.Rel(cwd, *task.BaseModel.Spec.Storage.Path)
				require.NoError(t, err)
				task.BaseModel.Spec.Storage.Path = &path
			}
			want := getDestPath(&task.BaseModel.Spec, g.modelRootDir)
			download := source.download
			source.download = func(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
				require.Equal(t, want, config.LocalDir)
				return download(ctx, task, config)
			}
			waiting, err := source.process(context.Background(), g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			require.FileExists(t, filepath.Join(want, "model.safetensors"))
		})
	}
}

func TestRestorationLocalReaderRemainsReadOnly(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "shared child"}[shared], func(t *testing.T) {
			g, target, input := newSharedEvictionTestModel(t)
			path := input.ChildModelPath
			if !shared {
				path = filepath.Join(g.modelRootDir, "local")
				require.NoError(t, os.MkdirAll(path, 0o700))
			}
			peer := sharedReaderPeer(t, g)
			model := target.BaseModel.DeepCopy()
			model.Name, model.UID = "reader", "reader"
			model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore"}
			model.Spec.Storage = &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}
			require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(model))
			task := &GopherTask{TaskType: Download, BaseModel: model}
			require.NoError(t, peer.processTask(task))
			entry := directArtifactEntry(t, peer, task)
			require.Equal(t, ModelStatusReady, entry.Status)
			require.Empty(t, entry.DirectArtifactPath)
			require.Empty(t, entry.HfArtifactKey)
		})
	}
}

func TestRestorationRequestFenceInputs(t *testing.T) {
	for _, change := range []string{"uid", "request", "source", "path", "policy", "intent", "placement"} {
		t.Run(change, func(t *testing.T) {
			g, task, _ := newDirectArtifactTestModel(t)
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
			task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
			latest := task.BaseModel.DeepCopy()
			switch change {
			case "uid":
				latest.UID = "replacement"
			case "request":
				latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "next"
			case "source":
				latest.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/other")
			case "path":
				latest.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, "other"))
			case "policy":
				policy := v1beta1.ReuseIfExists
				latest.Spec.Storage.DownloadPolicy = &policy
			case "intent":
				latest.Annotations[ArtifactResidencyAnnotation] = string(ModelStatusEvicted)
			case "placement":
				latest.Spec.Storage.NodeSelector = map[string]string{"kubernetes.io/hostname": "other-node"}
			}
			g.modelClient = omefake.NewSimpleClientset(latest)
			require.Error(t, g.validateArtifactDownload(context.Background(), task))
		})
	}
}

func TestRestorationRejectsInvalidDownloadCompletion(t *testing.T) {
	for _, completion := range []string{"bad bytes", "new request"} {
		t.Run(completion, func(t *testing.T) {
			g, task, _, source, _ := newTestDirectHfSource(t)
			_, err := g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
			require.NoError(t, err)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "old"
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			download := source.download
			downloaded := false
			source.download = func(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
				downloaded = true
				err := download(ctx, task, config)
				require.NoError(t, err)
				if completion == "bad bytes" {
					return os.WriteFile(filepath.Join(config.LocalDir, "model.safetensors"), []byte("CORRUPT"), 0o600)
				}
				latest := task.BaseModel.DeepCopy()
				latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new"
				return g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace)
			}
			_, err = source.process(context.Background(), g, task, task.BaseModel.Spec, true)
			require.Error(t, err)
			require.True(t, downloaded)
		})
	}
}

func TestRestorationHfRepairVerifiesWithdrawnReadiness(t *testing.T) {
	for _, shared := range []bool{false, true} {
		for _, corrupt := range []bool{false, true} {
			t.Run(map[bool]string{false: "direct/", true: "shared/"}[shared]+map[bool]string{false: "healthy reuse", true: "repair"}[corrupt], func(t *testing.T) {
				ctx := context.Background()
				g, task, input, source, downloads := newTestDirectHfSource(t)
				if !shared {
					task.BaseModel.Spec.Storage.DownloadPolicy = nil
				}
				waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
				require.NoError(t, err)
				require.False(t, waiting)
				path, contents := input.ChildModelPath, "weights"
				if shared {
					path = input.Parent.LocalPath
				}
				if corrupt {
					contents = "CORRUPT"
				}
				require.NoError(t, os.WriteFile(filepath.Join(path, "model.safetensors"), []byte(contents), 0o600))
				task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore"
				g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
				label := constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{label: string(Ready)}}}
				_, err = g.kubeClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
				require.NoError(t, err)
				g.nodeLabelReconciler = NewNodeLabelReconciler(node.Name, g.kubeClient, 1, g.logger)
				// Even a successful response is insufficient if Ready remains.
				g.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, node.DeepCopy(), nil
				})
				waiting, err = source.process(ctx, g, task, task.BaseModel.Spec, true)
				if !corrupt {
					require.NoError(t, err)
					require.False(t, waiting)
				}
				after, err := os.ReadFile(filepath.Join(path, "model.safetensors"))
				require.NoError(t, err)
				require.Equal(t, contents, string(after))
				require.Equal(t, 1, *downloads)
				node, err = g.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, string(Ready), node.Labels[label])
			})
		}
	}
}
