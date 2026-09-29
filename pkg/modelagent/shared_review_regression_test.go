package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestSharedEvictionRetriesOnlyConfirmedNodeVersionCompetition(t *testing.T) {
	for _, mode := range []string{"version changed", "invalid patch", "missing node", "intent revoked", "version keeps changing"} {
		t.Run(mode, func(t *testing.T) {
			s, task, input := newSharedEvictionFixture(t)
			client := s.kubeClient.(*fake.Clientset)
			patches := 0
			client.PrependReactor("patch", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				patches++
				if patches > 1 && mode != "version keeps changing" {
					return false, nil, nil
				}
				if mode == "missing node" {
					return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, s.configMapReconciler.nodeName)
				}
				if mode != "invalid patch" {
					obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", s.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := obj.(*corev1.Node).DeepCopy()
					node.ResourceVersion = fmt.Sprint(patches + 1)
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
				}
				if mode == "intent revoked" {
					model := task.BaseModel.DeepCopy()
					delete(model.Annotations, artifactResidencyAnnotation)
					_, err := s.omeClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{Code: 422, Reason: metav1.StatusReasonInvalid, Message: "JSON patch test failed"}}
			})
			err := (&artifactCleanupGuard{gopher: s, task: task}).withdrawReady(context.Background())
			if mode == "version changed" {
				require.NoError(t, err)
				require.Equal(t, 2, patches)
			} else {
				require.Error(t, err)
				if mode == "version keeps changing" {
					require.Greater(t, patches, 1)
					require.LessOrEqual(t, patches, 6)
				} else {
					require.Equal(t, 1, patches)
				}
				require.DirExists(t, input.Parent.LocalPath)
			}
		})
	}
}

func TestSharedRestoreRepairsEquivalentHFURIsUsingTrackedParent(t *testing.T) {
	for _, mode := range []string{"default then main", "main then default", "different model", "different commit", "changed path", "changed UID"} {
		t.Run(mode, func(t *testing.T) {
			s, task, input := newSharedRestoreFixture(t)
			task.TaskType = DownloadOverride
			task.BaseModel.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			if mode == "main then default" {
				task.BaseModel.Spec.Storage.StorageUri = ptr(*task.BaseModel.Spec.Storage.StorageUri + "@main")
			}
			_, err := s.omeClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Update(context.Background(), task.BaseModel, metav1.UpdateOptions{})
			require.NoError(t, err)
			sibling := task.BaseModel.DeepCopy()
			sibling.Name, sibling.UID = "sibling", types.UID("sibling-uid")
			sibling.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID + "@main")
			if mode == "main then default" {
				sibling.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID)
			}
			delete(sibling.Annotations, testRestoreAnnotation)
			sibling.Spec.Storage.Path = ptr(filepath.Join(input.ModelStoreRoot, "sibling"))
			second := input
			second.ChildModelKey, second.ChildModelUID, second.ChildModelPath = getModelID(sibling, nil), sibling.UID, *sibling.Spec.Storage.Path
			seedTestChildModelEntry(t, s.sharedHfArtifactHandler().repository, second)
			require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), second))
			switch mode {
			case "different model":
				sibling.Spec.Storage.StorageUri = ptr("hf://other/model@main")
			case "different commit":
				sibling.Spec.Storage.StorageUri = ptr("hf://" + input.Parent.Identity.ModelID + "@" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			case "changed path":
				sibling.Spec.Storage.Path = ptr(filepath.Join(input.ModelStoreRoot, "moved"))
			case "changed UID":
				sibling.UID = "replacement"
			}
			_, err = s.omeClient.OmeV1beta1().BaseModels(sibling.Namespace).Create(context.Background(), sibling, metav1.CreateOptions{})
			require.NoError(t, err)
			downloads := 0
			result, err := s.runHfArtifactDownload(context.Background(), task, input, true, func(string) (bool, error) { return false, nil }, func(path string) error { downloads++; return writeTestHfArtifactFiles(path) })
			if mode == "default then main" || mode == "main then default" {
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome, "%+v", result)
				require.Equal(t, 1, downloads)
			} else {
				require.True(t, err != nil || result.Outcome != hfArtifactTaskDone, "incompatible current model must protect its tracked link")
				require.Zero(t, downloads)
				_, err = os.Lstat(second.ChildModelPath)
				require.NoError(t, err)
			}
		})
	}
}
