package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

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
			_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, UID: "node-uid", Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName}}}, metav1.CreateOptions{})
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
