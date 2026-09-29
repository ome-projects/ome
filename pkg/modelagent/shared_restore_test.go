package modelagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

const testRestoreAnnotation = "ome.io/artifact-rehydration-id"

func newSharedRestoreFixture(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput) {
	t.Helper()
	s, task, input := newSharedEvictionFixture(t)
	delete(task.BaseModel.Annotations, artifactResidencyAnnotation)
	task.BaseModel.Annotations[testRestoreAnnotation] = "request-1"
	task.TaskType = Download
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	return s, task, input
}

func testRequestLabel(task *GopherTask) string {
	digest := sha256.Sum256([]byte(getModelUID(task)))
	return fmt.Sprintf("ome.io/artifact-%x", digest[:24])
}

func TestSharedRestoreHealthyReuseReportsBeforeLabels(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	ctx := context.Background()
	validations := 0
	patches := 0
	s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		patches++
		object, err := s.kubeClient.(*fake.Clientset).Tracker().Get(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, s.configMapReconciler.namespace, s.configMapReconciler.nodeName)
		require.NoError(t, err)
		cm := object.(*corev1.ConfigMap)
		var report map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(cm.Data[input.ChildModelKey]), &report))
		require.Equal(t, "request-1", report["artifactRehydrationID"], "report must commit before labels")
		require.Equal(t, "node-uid", report["nodeUID"])
		require.Equal(t, string(input.ChildModelUID), report["modelUID"])
		return false, nil, nil
	})
	result, err := s.runHfArtifactDownload(ctx, task, input, true, func(string) (bool, error) {
		validations++
		return true, nil
	}, func(string) error { t.Fatal("healthy parent must not download"); return nil })
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
	require.Equal(t, 1, validations, "each new request validates existing bytes")
	require.Positive(t, patches, "old Ready alone does not acknowledge a new request")
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "request-1", node.Labels[testRequestLabel(task)])
}

func TestSharedRestoreRejectsStaleAuthority(t *testing.T) {
	for _, change := range []string{"request", "empty queued request", "model", "source", "path", "placement", "node", "invalid request"} {
		t.Run(change, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			current := task.BaseModel.DeepCopy()
			switch change {
			case "request":
				current.Annotations[testRestoreAnnotation] = "request-2"
			case "empty queued request":
				delete(task.BaseModel.Annotations, testRestoreAnnotation)
			case "model":
				current.UID = "replacement"
			case "source":
				current.Spec.Storage.StorageUri = ptr("oci://n/ns/b/models/o/other")
			case "path":
				current.Spec.Storage.Path = ptr(filepath.Join(input.ModelStoreRoot, "other"))
			case "placement":
				current.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
			case "node":
				node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				node.UID = "replacement"
				_, err = s.kubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "invalid request":
				current.Annotations[testRestoreAnnotation] = "invalid/request"
				task.BaseModel.Annotations[testRestoreAnnotation] = "invalid/request"
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.Error(t, s.validateArtifactDownload(context.Background(), task))
		})
	}
}

func TestScoutSharedRestoreUsesNormalDownload(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, kind := range []string{"request", "unevict", "source override", "other annotation override"} {
			t.Run(fmt.Sprintf("%v/%s", cluster, kind), func(t *testing.T) {
				scout, tasks := newScoutForUpdateTest(t)
				old := newBaseModel("model", v1beta1.ReuseIfExists, "hf://org/model")
				old.Annotations = map[string]string{testRestoreAnnotation: "request-1"}
				model := old.DeepCopy()
				model.Annotations[testRestoreAnnotation] = "request-2"
				want := Download
				switch kind {
				case "unevict":
					old.Annotations[artifactResidencyAnnotation] = "Evicted"
				case "source override":
					model.Spec.Storage.StorageUri = ptr("hf://other/model")
					want = DownloadOverride
				case "other annotation override":
					model.Annotations["other"] = "changed"
					want = DownloadOverride
				}
				checkScoutUpdateTask(t, scout, tasks, cluster, old, model, want)
			})
		}
	}
}

func TestSharedRestoreRefusesCorruptBorrowedParent(t *testing.T) {
	for _, source := range []string{"local", "direct", "unrecorded symlink"} {
		t.Run(source, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			borrower := task.BaseModel.DeepCopy()
			borrower.Name, borrower.UID = "borrower", "borrower-uid"
			borrower.Annotations = nil
			borrower.Spec.Storage.Path = ptr(input.Parent.LocalPath)
			if source == "local" {
				borrower.Spec.Storage.StorageUri = ptr("local://" + input.Parent.LocalPath)
			}
			if source == "unrecorded symlink" {
				require.NoError(t, os.Symlink(input.Parent.LocalPath, filepath.Join(input.ModelStoreRoot, "unknown-child")))
			} else {
				_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(context.Background(), borrower, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			result, err := s.runHfArtifactDownload(context.Background(), task, input, true, func(string) (bool, error) { return false, nil }, func(string) error { t.Fatal("borrowed bytes must not be overwritten"); return nil })
			require.True(t, err != nil || result.Outcome == hfArtifactTaskRetry, "corrupt parent needs safe retry, got %+v", result)
			require.DirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedRestoreRepairsTrackedSiblings(t *testing.T) {
	for _, mode := range []string{"corrupt", "missing", "withdrawal failure"} {
		t.Run(mode, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			sibling := task.BaseModel.DeepCopy()
			sibling.Name, sibling.UID = "sibling", "sibling-uid"
			delete(sibling.Annotations, testRestoreAnnotation)
			sibling.Spec.Storage.Path = ptr(filepath.Join(input.ModelStoreRoot, "sibling"))
			_, err := s.omeClient.OmeV1beta1().BaseModels(sibling.Namespace).Create(context.Background(), sibling, metav1.CreateOptions{})
			require.NoError(t, err)
			second := input
			second.ChildModelKey, second.ChildModelUID, second.ChildModelPath = getModelID(sibling, nil), sibling.UID, *sibling.Spec.Storage.Path
			seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, second)
			require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), second))
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(context.Background(), &ConfigMapStatusOp{BaseModel: sibling, ModelStatus: ModelStatusReady}))
			if mode == "missing" {
				require.NoError(t, os.RemoveAll(input.Parent.LocalPath))
			}
			if mode == "withdrawal failure" {
				s.kubeClient.(*fake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewBadRequest("readiness patch rejected")
				})
			}
			downloads := 0
			result, err := s.runHfArtifactDownload(context.Background(), task, input, true, func(string) (bool, error) { return false, nil }, func(path string) error { downloads++; return writeTestHfArtifactFiles(path) })
			if mode == "withdrawal failure" {
				require.True(t, err != nil || result.Outcome == hfArtifactTaskRetry)
				require.Zero(t, downloads)
				return
			}
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
			assertChildSymlinkTarget(t, second.ChildModelPath, input.Parent.LocalPath)
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, second.ChildModelKey)
			require.NoError(t, err)
			require.Equal(t, ModelStatusReady, entry.Status)
		})
	}
}

func TestSharedRestoreAttachmentLostResponseAcrossRestart(t *testing.T) {
	s, task, input := newSharedEvictionFixture(t)
	ctx := context.Background()
	require.NoError(t, s.processTask(task))
	task = &GopherTask{TaskType: Download, BaseModel: task.BaseModel.DeepCopy()}
	delete(task.BaseModel.Annotations, artifactResidencyAnnotation)
	task.BaseModel.Annotations[testRestoreAnnotation] = "request-1"
	_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
	require.NoError(t, err)
	client := s.kubeClient.(*fake.Clientset)
	injected := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
		require.NoError(t, err)
		if injected || entry.HfArtifactKey == "" {
			return false, nil, nil
		}
		injected = true
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		return true, nil, errors.New("attachment response lost")
	})
	// Build from current policy, without carrying former ownership or layout.
	input, eligible, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, s.modelRootDir)
	require.NoError(t, err)
	require.True(t, eligible)
	result, err := s.runHfArtifactDownload(ctx, task, input, true, func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskRetry, result.Outcome)
	require.True(t, injected)
	s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.kubeClient, s.logger)
	WithArtifactEviction(s.omeClient, s.artifactNodeUID)(s)
	s.hfArtifactHandlerOnce = sync.Once{}
	s.sharedHfArtifactHandler()
	result, err = s.runHfArtifactDownload(ctx, task, input, true, func(string) (bool, error) { return true, nil }, func(string) error { t.Fatal("valid attached bytes must survive restart"); return nil })
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
	assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
}

func TestSharedRestoreDeletePreservesReplacementReady(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	ctx := context.Background()
	result, err := s.runHfArtifactDownload(ctx, task, input, true, func(string) (bool, error) { return true, nil }, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	task.TaskType = Delete
	_, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
	require.NoError(t, err)
	require.False(t, waiting)
	replacement := task.BaseModel.DeepCopy()
	replacement.UID = "replacement"
	_, err = s.omeClient.OmeV1beta1().BaseModels(replacement.Namespace).Update(ctx, replacement, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted}))
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	key, err := getModelLabelKey(&NodeLabelOp{BaseModel: replacement})
	require.NoError(t, err)
	require.Equal(t, "Ready", node.Labels[key])
	require.Empty(t, node.Labels[testRequestLabel(task)])
}

func TestSharedRestoreAfterEvictionAndCleanupRestart(t *testing.T) {
	for _, stage := range []string{"completed", "pending", "pending changed path"} {
		t.Run(stage, func(t *testing.T) {
			s, task, oldInput := newSharedEvictionFixture(t)
			ctx := context.Background()
			if stage == "completed" {
				require.NoError(t, s.processTask(task))
			} else {
				_, err := s.sharedHfArtifactHandler().repository.removeModelReference(ctx, oldInput.Parent, oldInput.ChildModelKey, oldInput.ChildModelUID, oldInput.ChildModelPath, true)
				require.NoError(t, err)
			}
			task = &GopherTask{TaskType: Download, BaseModel: task.BaseModel.DeepCopy()}
			delete(task.BaseModel.Annotations, artifactResidencyAnnotation)
			task.BaseModel.Annotations[testRestoreAnnotation] = "request-1"
			if stage == "pending changed path" {
				task.BaseModel.Spec.Storage.Path = ptr(filepath.Join(oldInput.ModelStoreRoot, "new-child"))
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			s.configMapReconciler = NewConfigMapReconciler(s.configMapReconciler.nodeName, s.configMapReconciler.namespace, s.kubeClient, s.logger)
			WithArtifactEviction(s.omeClient, s.artifactNodeUID)(s)
			s.hfArtifactHandlerOnce, s.artifactRouting = sync.Once{}, gopherArtifactRouting{}
			s.sharedHfArtifactHandler()
			_, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			require.NoError(t, err)
			require.False(t, waiting)
			input, eligible, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, s.modelRootDir)
			require.NoError(t, err)
			require.True(t, eligible)
			result, err := s.runHfArtifactDownload(ctx, task, input, true, func(string) (bool, error) { return false, nil }, writeTestHfArtifactFiles)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			if stage == "pending changed path" {
				assertChildPathMissing(t, oldInput.ChildModelPath)
			}
			cm, err := s.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, entry.HfArtifactPendingDeletion)
			require.Equal(t, "request-1", entry.ArtifactRehydrationID)
			require.NotContains(t, cm.Data[input.ChildModelKey], "artifactLayout")
		})
	}
}

func TestSharedRestoreMissingParentDownloads(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	require.NoError(t, os.RemoveAll(input.Parent.LocalPath))
	downloads := 0
	result, err := s.runHfArtifactDownload(context.Background(), task, input, true, func(string) (bool, error) { return false, nil }, func(path string) error { downloads++; return writeTestHfArtifactFiles(path) })
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
	require.Equal(t, 1, downloads)
	node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "request-1", node.Labels[testRequestLabel(task)])
}

func TestSharedRestoreReportRetriesRecheckAuthority(t *testing.T) {
	for _, change := range []string{"request", "model", "node", "source", "path", "placement", "evicted"} {
		t.Run(change, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			writes := 0
			s.kubeClient.(*fake.Clientset).PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
				require.NoError(t, err)
				if entry.ArtifactRehydrationID != "request-1" || entry.Status != ModelStatusReady {
					return false, nil, nil
				}
				writes++
				current := task.BaseModel.DeepCopy()
				switch change {
				case "request":
					current.Annotations[testRestoreAnnotation] = "request-2"
				case "model":
					current.UID = "replacement"
				case "source":
					current.Spec.Storage.StorageUri = ptr("oci://n/ns/b/models/o/other")
				case "path":
					current.Spec.Storage.Path = ptr(filepath.Join(input.ModelStoreRoot, "other"))
				case "placement":
					current.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
				case "evicted":
					current.Annotations[artifactResidencyAnnotation] = "Evicted"
				case "node":
					object, err := s.kubeClient.(*fake.Clientset).Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := object.(*corev1.Node)
					node.UID = "replacement"
					require.NoError(t, s.kubeClient.(*fake.Clientset).Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
				}
				_, err = s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
				require.NoError(t, err)
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, errors.New("conflict"))
			})
			result, err := s.runHfArtifactDownload(context.Background(), task, input, true, func(string) (bool, error) { return true, nil }, nil)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			require.Equal(t, 1, writes, "stale report must not retry the write")
			node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Empty(t, node.Labels[testRequestLabel(task)])
		})
	}
}

func TestSharedRestoreLostResponses(t *testing.T) {
	for _, stage := range []string{"report", "labels", "labels changed request", "labels replaced node"} {
		t.Run(stage, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			client := s.kubeClient.(*fake.Clientset)
			s.nodeLabelReconciler.opRetry = 2
			injected := false
			if stage == "report" {
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
					require.NoError(t, err)
					if injected || entry.ArtifactRehydrationID != "request-1" {
						return false, nil, nil
					}
					injected = true
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					return true, nil, errors.New("lost report response")
				})
			} else {
				client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
					if injected {
						return false, nil, nil
					}
					injected = true
					var patch []map[string]interface{}
					require.NoError(t, json.Unmarshal(action.(ktesting.PatchAction).GetPatch(), &patch))
					require.Equal(t, "/metadata/uid", patch[0]["path"])
					require.Equal(t, "/metadata/resourceVersion", patch[1]["path"])
					object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := object.(*corev1.Node)
					key, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel})
					require.NoError(t, err)
					node.Labels[key], node.Labels[testRequestLabel(task)] = "Ready", "request-1"
					if stage == "labels replaced node" {
						node.UID = "replacement"
						node.Labels = map[string]string{}
					}
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
					if stage == "labels changed request" {
						current := task.BaseModel.DeepCopy()
						current.Annotations[testRestoreAnnotation] = "request-2"
						_, err := s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(context.Background(), current, metav1.UpdateOptions{})
						require.NoError(t, err)
					}
					return true, nil, errors.New("lost label response")
				})
			}
			validate := func(string) (bool, error) { return true, nil }
			result, err := s.runHfArtifactDownload(context.Background(), task, input, true, validate, nil)
			require.NoError(t, err)
			require.True(t, injected)
			if stage == "report" {
				require.Equal(t, hfArtifactTaskRetry, result.Outcome)
				// A new process has no validation proof; retry validates again.
				s.hfArtifactHandlerOnce = sync.Once{}
				s.sharedHfArtifactHandler()
				result, err = s.runHfArtifactDownload(context.Background(), task, input, true, validate, nil)
				require.NoError(t, err)
			}
			if stage == "labels changed request" || stage == "labels replaced node" {
				require.Equal(t, hfArtifactTaskRetry, result.Outcome)
			} else {
				require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
			}
		})
	}
}

func TestSharedRestoreGenericReadyCannotAcknowledge(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	ctx := context.Background()
	require.Error(t, s.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready}))
	require.NoError(t, s.updateHfArtifactChildLabels(ctx, map[string]ModelStatus{input.ChildModelKey: ModelStatusReady}))
	require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: task.BaseModel, ModelStatus: ModelStatusReady}))
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	require.NoError(t, err)
	entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
	require.NoError(t, err)
	require.NotEqual(t, ModelStatusReady, entry.Status)
	require.Empty(t, entry.ArtifactRehydrationID)
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, node.Labels[testRequestLabel(task)])
}

func TestSharedRestoreOrdinaryDeleteRemovesRequestLabel(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	result, err := s.runHfArtifactDownload(context.Background(), task, input, true, func(string) (bool, error) { return true, nil }, nil)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome)
	task.TaskType = Delete
	handled, waiting, err := s.processSharedHfArtifactDelete(context.Background(), task)
	require.NoError(t, err)
	require.True(t, handled)
	require.False(t, waiting)
	require.NoError(t, s.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted}))
	node, err := s.kubeClient.CoreV1().Nodes().Get(context.Background(), s.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, node.Labels[testRequestLabel(task)])
}

func TestSharedRestoreProtectsDanglingUnrecordedAliases(t *testing.T) {
	for _, mode := range []string{"parent missing", "descendant missing", "sibling prefix"} {
		t.Run(mode, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			target := input.ChildModelPath
			switch mode {
			case "parent missing":
				require.NoError(t, os.RemoveAll(input.Parent.LocalPath))
			case "descendant missing":
				target = filepath.Join(target, "missing-descendant")
			case "sibling prefix":
				target += "-other"
			}
			require.NoError(t, os.Symlink(target, filepath.Join(input.ModelStoreRoot, "unmanaged-alias")))
			downloads := 0
			result, err := s.runHfArtifactDownload(context.Background(), task, input, true,
				func(string) (bool, error) { return false, nil },
				func(path string) error { downloads++; return writeTestHfArtifactFiles(path) })
			if mode == "sibling prefix" {
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome)
				require.Equal(t, 1, downloads)
				return
			}
			require.Zero(t, downloads, "unrecorded alias must prevent parent writes: result=%+v, err=%v", result, err)
			require.True(t, err != nil || result.Outcome == hfArtifactTaskRetry)
		})
	}
}

func TestSharedRestoreDeleteResumesReceiptAfterCRRemoval(t *testing.T) {
	for _, state := range []string{"absent", "deleting"} {
		t.Run(state, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			result, err := s.runHfArtifactDownload(ctx, task, input, true, func(string) (bool, error) { return true, nil }, nil)
			require.NoError(t, err)
			require.Equal(t, hfArtifactTaskDone, result.Outcome)
			_, err = s.sharedHfArtifactHandler().repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			if state == "absent" {
				require.NoError(t, s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Delete(ctx, task.BaseModel.Name, metav1.DeleteOptions{}))
			} else {
				current := task.BaseModel.DeepCopy()
				now := metav1.Now()
				current.DeletionTimestamp = &now
				_, err = s.omeClient.OmeV1beta1().BaseModels(current.Namespace).Update(ctx, current, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			task.TaskType = Delete
			handled, waiting, err := s.processSharedHfArtifactDelete(ctx, task)
			require.NoError(t, err, "ordinary Delete must use its cleanup receipt despite a retained request annotation")
			require.True(t, handled)
			require.False(t, waiting)
			assertChildPathMissing(t, input.ChildModelPath)
			require.NoDirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestSharedRestoreProtectsBorrowersBelowMissingChildDescendants(t *testing.T) {
	for _, source := range []string{"local", "direct", "persisted parent", "persisted child", "pending receipt", "sibling prefix"} {
		t.Run(source, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			path := filepath.Join(input.ChildModelPath, "missing-descendant")
			if source == "sibling prefix" {
				path = filepath.Join(input.ChildModelPath+"-other", "missing-descendant")
			}
			borrower := task.BaseModel.DeepCopy()
			borrower.Name, borrower.UID = "borrower", "borrower-uid"
			borrower.Annotations = nil
			borrower.Spec.Storage.Path = ptr(path)
			switch source {
			case "persisted parent", "persisted child", "pending receipt":
				entry := ModelEntry{Name: borrower.Name, Status: ModelStatusUpdating, Config: &ModelConfig{}}
				switch source {
				case "persisted parent":
					entry.Config.Artifact.ParentPath = map[string]string{"previous": path}
				case "persisted child":
					entry.Config.Artifact.ChildrenPaths = []string{path}
				case "pending receipt":
					entry.HfArtifactPendingDeletion = &HfArtifactPendingDeletion{Identity: input.Parent.Identity, ParentPath: path, ChildPath: filepath.Join(input.ModelStoreRoot, "borrower"), ModelUID: borrower.UID}
				}
				require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
					return writeModelEntry(cm.Data, getModelID(borrower, nil), entry)
				}))
			default:
				if source == "local" {
					borrower.Spec.Storage.StorageUri = ptr("local://" + path)
					borrower.Spec.Storage.Path = nil
				}
				_, err := s.omeClient.OmeV1beta1().BaseModels(borrower.Namespace).Create(ctx, borrower, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			downloads := 0
			result, err := s.runHfArtifactDownload(ctx, task, input, true,
				func(string) (bool, error) { return false, nil },
				func(path string) error { downloads++; return writeTestHfArtifactFiles(path) })
			if source == "sibling prefix" {
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome)
				require.Equal(t, 1, downloads)
				return
			}
			require.Zero(t, downloads, "%s borrower below a managed child must prevent repair: result=%+v, err=%v", source, result, err)
			require.True(t, err != nil || result.Outcome == hfArtifactTaskRetry)
		})
	}
}

func TestSharedRestoreOrdinaryReplacementReceiptWithLiveClient(t *testing.T) {
	for _, request := range []string{"", "request-2"} {
		t.Run("request="+request, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			ctx := context.Background()
			h := s.sharedHfArtifactHandler()
			_, err := h.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
			require.NoError(t, err)
			task.TaskType = Download
			task.BaseModel = task.BaseModel.DeepCopy()
			task.BaseModel.Annotations = map[string]string{testRestoreAnnotation: request}
			task.BaseModel.UID = "replacement-uid"
			_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			require.NoError(t, indexer.Add(task.BaseModel))
			s.baseModelLister = modelslister.NewBaseModelLister(indexer)
			handled, waiting, err := s.resumeHfArtifactChildDeletion(ctx, task, true)
			if request != "" {
				require.ErrorContains(t, err, "another model UID")
				pending, lookupErr := h.repository.pendingDeletion(ctx, input.ChildModelKey)
				require.NoError(t, lookupErr)
				require.NotNil(t, pending)
				require.Equal(t, input.ChildModelUID, pending.ModelUID)
				return
			}
			require.NoError(t, err, "ordinary replacement must retain the existing fenced receipt handoff with the production client enabled")
			require.True(t, handled)
			require.False(t, waiting)
			pending, err := h.repository.pendingDeletion(ctx, input.ChildModelKey)
			require.NoError(t, err)
			require.Nil(t, pending)
			assertChildPathMissing(t, input.ChildModelPath)
		})
	}
}
