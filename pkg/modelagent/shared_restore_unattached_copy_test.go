package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestSharedRestoreAfterEvictionWithUnattachedCopy(t *testing.T) {
	for _, scenario := range []string{"queued copy", "copy during Updating", "copy queued with Updating entry", "stale copy entry", "different source", "different policy", "retained directory", "unrecorded link", "alias through child", "alias through descendant", "pending cleanup"} {
		t.Run(scenario, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			ctx := context.Background()
			s.metrics = NewMetrics(prometheus.NewRegistry())
			s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			task.BaseModel.Annotations[artifactResidencyAnnotation] = "Evicted"
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, s.processTask(&GopherTask{TaskType: Evict, BaseModel: task.BaseModel.DeepCopy()}))
			require.NoDirExists(t, input.Parent.LocalPath)
			assertChildPathMissing(t, input.ChildModelPath)

			delete(task.BaseModel.Annotations, artifactResidencyAnnotation)
			task.BaseModel.Annotations[testRestoreAnnotation] = "request-2"
			_, err = s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(ctx, task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			copy := task.BaseModel.DeepCopy()
			copy.Namespace, copy.UID = "dac-x", "copy-uid"
			delete(copy.Annotations, testRestoreAnnotation)
			switch scenario {
			case "different source":
				copy.Spec.Storage.StorageUri = ptr("hf://different/model")
			case "different policy":
				copy.Spec.Storage.DownloadPolicy = ptr(v1beta1.AlwaysDownload)
			case "retained directory":
				require.NoError(t, os.MkdirAll(input.Parent.LocalPath, 0o755))
			case "alias through child", "alias through descendant":
				target := input.ChildModelPath
				if scenario == "alias through descendant" {
					target = filepath.Join(target, "config.json")
				}
				require.NoError(t, os.Symlink(target, filepath.Join(input.ModelStoreRoot, "unknown")))
			case "unrecorded link":
				require.NoError(t, os.Symlink(input.Parent.LocalPath, filepath.Join(input.ModelStoreRoot, "unknown")))
			}
			createCopy := func() {
				_, err := s.omeClient.OmeV1beta1().BaseModels(copy.Namespace).Create(ctx, copy, metav1.CreateOptions{})
				require.NoError(t, err)
				s.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, copy}}
			}
			if scenario != "copy during Updating" {
				createCopy()
			}
			if scenario == "copy queued with Updating entry" || scenario == "stale copy entry" {
				uid := copy.UID
				if scenario == "stale copy entry" {
					uid = "old-copy"
				}
				require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
					return writeModelEntry(cm.Data, getModelID(copy, nil), ModelEntry{Name: copy.Name, ModelUID: uid, NodeUID: s.artifactNodeUID, Status: ModelStatusUpdating})
				}))
			}
			if scenario == "pending cleanup" {
				require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
					return writeModelEntry(cm.Data, getModelID(copy, nil), ModelEntry{Name: copy.Name, ModelUID: copy.UID,
						HfArtifactPendingDeletion: &HfArtifactPendingDeletion{ModelUID: copy.UID, ChildPath: input.ChildModelPath, ParentPath: input.Parent.LocalPath, Identity: input.Parent.Identity}})
				}))
			}
			// Process the download-capable worker stage, after any OCI fast-lane demotion.
			task.NormalPriorityOnly = true
			task.SamePathWaitStartedAt = time.Time{}
			s.enqueueTask(task)
			queued, ok := s.taskQueue.popNormal()
			require.True(t, ok)
			queued.SamePathWaitStartedAt = time.Now().Add(-time.Minute)
			downloads := 0
			err = s.processTaskWithSourceAdapters(queued, true,
				func(ctx context.Context, current *GopherTask, _ v1beta1.BaseModelSpec, allow bool) (bool, error) {
					if scenario == "copy during Updating" {
						cm, err := s.configMapReconciler.getConfigMap(ctx)
						require.NoError(t, err)
						entry, err := existingModelEntry(cm.Data, input.ChildModelKey)
						require.NoError(t, err)
						require.Equal(t, ModelStatusUpdating, entry.Status)
						createCopy()
					}
					result, err := s.runHfArtifactDownload(ctx, current, input, allow,
						func(string) (bool, error) { return false, nil },
						func(path string) error { downloads++; return writeTestHfArtifactFiles(path) })
					if err == nil && result.Outcome == hfArtifactTaskRetry {
						return true, s.requeueHfArtifactTask(current, result)
					}
					return false, err
				}, nil)
			if scenario != "queued copy" && scenario != "copy during Updating" && scenario != "copy queued with Updating entry" {
				require.Error(t, err)
				require.Zero(t, downloads)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, downloads)
			require.NoError(t, s.validateArtifactRestoreReport(ctx, queued))
		})
	}
}

func TestUnattachedSharedOCICopyIdentity(t *testing.T) {
	s, task, input := newSharedRestoreFixture(t)
	copy := task.BaseModel.DeepCopy()
	copy.Namespace, copy.UID = "dac-x", "copy-uid"
	candidate := &GopherTask{TaskType: Download, BaseModel: copy}
	require.True(t, s.isUnattachedSharedCopy(candidate, task, input, nil))
	copy.Annotations[hfSHAAnnotationKey] = "different-commit"
	require.False(t, s.isUnattachedSharedCopy(candidate, task, input, nil))
}
