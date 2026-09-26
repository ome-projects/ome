package modelagent

import (
	"context"
	"os"
	"path/filepath"
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
)

func TestRestorationRepairRechecksBorrowerAfterWithdrawal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		shared    bool
		persisted bool
	}{
		{name: "direct live"},
		{name: "shared live", shared: true},
		{name: "direct persisted", persisted: true},
		{name: "shared persisted", shared: true, persisted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			g, task, input, source, downloads := newTestDirectHfSource(t)
			if !tc.shared {
				task.BaseModel.Spec.Storage.DownloadPolicy = nil
			}
			waiting, err := source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			require.False(t, waiting)
			require.Equal(t, 1, *downloads)
			path := input.ChildModelPath
			if tc.shared {
				path = input.Parent.LocalPath
			}
			require.NoError(t, os.WriteFile(filepath.Join(path, "model.safetensors"), []byte("CORRUPT"), 0600))
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore"
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{
				constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name): string(Ready),
			}}}
			_, err = g.kubeClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
			require.NoError(t, err)
			g.nodeLabelReconciler = NewNodeLabelReconciler(node.Name, g.kubeClient, 1, g.logger)
			changed := false
			client := g.kubeClient.(*k8sfake.Clientset)
			client.PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
				if !changed {
					changed = true
					borrower := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "late-borrower", Namespace: "default", UID: "late-borrower"},
						Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}}}
					if tc.persisted {
						resource := corev1.SchemeGroupVersion.WithResource("configmaps")
						current, err := client.Tracker().Get(resource, g.configMapReconciler.namespace, g.configMapReconciler.nodeName)
						require.NoError(t, err)
						cm := current.(*corev1.ConfigMap)
						_, err = writeModelEntry(cm.Data, getModelID(borrower, nil), ModelEntry{ModelUID: borrower.UID,
							Status: ModelStatusReady, DirectArtifactPath: path})
						require.NoError(t, err)
						require.NoError(t, client.Tracker().Update(resource, cm, cm.Namespace))
					} else {
						require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(borrower))
					}
				}
				return false, nil, nil
			})
			waiting, err = source.process(ctx, g, task, task.BaseModel.Spec, true)
			require.True(t, changed)
			contents, readErr := os.ReadFile(filepath.Join(path, "model.safetensors"))
			require.NoError(t, readErr)
			require.Equal(t, "CORRUPT", string(contents), "late borrower must protect the original bytes")
			require.Equal(t, 1, *downloads)
			require.True(t, waiting || err != nil)
		})
	}
}
