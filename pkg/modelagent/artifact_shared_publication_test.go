package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestSharedPublicationNoRequestRejectsCompletedEviction(t *testing.T) {
	for _, sourceType := range []string{"oci", "hf"} {
		t.Run(sourceType, func(t *testing.T) {
			ctx, release := withDirectArtifactDownloadOperation(context.Background())
			defer release()
			g, task, input, source, _ := newTestDirectHfSource(t)
			if sourceType == "oci" {
				g, task, input = newTestHfArtifactGopher(t)
				result, err := g.runHfArtifactDownload(ctx, task, input, true, nil, func(path string) error {
					return os.MkdirAll(path, 0755)
				})
				require.NoError(t, err)
				require.Equal(t, hfArtifactTaskDone, result.Outcome)
			} else {
				waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
				require.NoError(t, err)
				require.False(t, waiting)
			}
			task.SharedArtifact = true
			g.samePathWaitDelay = time.Millisecond
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
			require.NoError(t, err)
			require.True(t, isSharedHfArtifactSymlink(input.ChildModelPath))
			live := task.BaseModel.DeepCopy()
			live.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
			g.modelClient = omefake.NewSimpleClientset(live)
			// A second agent completes eviction after this download's final
			// stale-task check but before its Ready publication.
			waiting, err := g.processArtifactEviction(context.Background(), &GopherTask{TaskType: Evict, BaseModel: live})
			require.NoError(t, err)
			require.False(t, waiting)
			_, err = os.Lstat(input.ChildModelPath)
			require.True(t, os.IsNotExist(err))
			cm, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.True(t, hasModelEntryStatus(cm.Data[input.ChildModelKey], ModelStatusEvicted))
			published, err := g.finishDownloadStatus(ctx, task, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
			require.NoError(t, err)
			require.False(t, published, "a completed shared download cannot become direct when its reference and path disappear")
			cm, err = g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			require.True(t, hasModelEntryStatus(cm.Data[input.ChildModelKey], ModelStatusEvicted))
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotEqual(t, "Ready", node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
			select {
			case retry := <-g.gopherChan:
				require.Same(t, task, retry)
			case <-time.After(time.Second):
				t.Fatal("lost shared publication proof must requeue the task")
			}
		})
	}
}

func TestSharedPublicationKeepsDirectFallback(t *testing.T) {
	for _, outsideRoot := range []bool{false, true} {
		name := "bounded"
		if outsideRoot {
			name = "outside-root"
		}
		t.Run(name, func(t *testing.T) {
			g, task, _, source, _ := newTestDirectHfSource(t)
			task.SharedArtifact = true
			if outsideRoot {
				task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(t.TempDir(), "legacy"))
			}
			path := *task.BaseModel.Spec.Storage.Path
			require.NoError(t, os.MkdirAll(path, 0755))
			marker := filepath.Join(path, "preserved")
			require.NoError(t, os.WriteFile(marker, []byte("legacy"), 0600))
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			ctx, release := withDirectArtifactDownloadOperation(context.Background())
			defer release()
			_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid"}}, metav1.CreateOptions{})
			require.NoError(t, err)
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			require.False(t, isSharedHfArtifactSymlink(path))
			published, err := g.finishDownloadStatus(ctx, task, &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Ready})
			require.NoError(t, err)
			require.True(t, published, "reuse eligibility is not evidence that shared download completed")
			require.FileExists(t, marker)
		})
	}
}
