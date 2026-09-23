package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestArtifactRepairConsumersAreNodeLocal(t *testing.T) {
	for _, tc := range []struct {
		name        string
		node        string
		phase       corev1.PodPhase
		terminating bool
		wantLocal   bool
		wantGlobal  bool
	}{
		{name: "running local", node: "node1", phase: corev1.PodRunning, wantLocal: true, wantGlobal: true},
		{name: "pending local", node: "node1", phase: corev1.PodPending, wantLocal: true, wantGlobal: true},
		{name: "terminating local", node: "node1", phase: corev1.PodRunning, terminating: true, wantLocal: true, wantGlobal: true},
		{name: "running other node", node: "node2", phase: corev1.PodRunning, wantGlobal: true},
		{name: "pending unassigned", phase: corev1.PodPending, wantGlobal: true},
		{name: "succeeded local", node: "node1", phase: corev1.PodSucceeded},
		{name: "failed local", node: "node1", phase: corev1.PodFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "serving"},
				Spec: corev1.PodSpec{NodeName: tc.node, Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: path},
				}}}}, Status: corev1.PodStatus{Phase: tc.phase},
			}
			if tc.terminating {
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			}
			client := k8sfake.NewSimpleClientset(pod)
			g := &Gopher{kubeClient: client, configMapReconciler: &ConfigMapReconciler{nodeName: "node1"}}
			used, err := g.pathHasLocalPodConsumers(context.Background(), path)
			require.NoError(t, err)
			require.Equal(t, tc.wantLocal, used)
			action := client.Actions()[0].(k8stesting.ListAction)
			require.Equal(t, "spec.nodeName=node1", action.GetListRestrictions().Fields.String())
			canonicalPath, err := canonicalModelReferencePath(path)
			require.NoError(t, err)
			used, err = g.pathHasPodConsumers(context.Background(), canonicalPath, "")
			require.NoError(t, err)
			require.Equal(t, tc.wantGlobal, used, "eviction remains conservative across nodes")
		})
	}
}

func TestArtifactRepairConsumersRequireNodeIdentity(t *testing.T) {
	for _, reconciler := range []*ConfigMapReconciler{nil, {}} {
		client := k8sfake.NewSimpleClientset()
		g := &Gopher{kubeClient: client, configMapReconciler: reconciler}
		_, err := g.pathHasLocalPodConsumers(context.Background(), t.TempDir())
		require.ErrorContains(t, err, "node identity")
		require.Empty(t, client.Actions())
	}
}

func TestArtifactRepairConsumersHFPaths(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			node    string
			healthy bool
			blocked bool
		}{
			{name: "local invalid copy blocks repair", node: "local", blocked: true},
			{name: "other node permits repair", node: "other-node"},
			{name: "unassigned demand permits repair"},
			{name: "local healthy copy is reusable", node: "local", healthy: true},
		} {
			t.Run(map[bool]string{false: "shared/", true: "direct/"}[direct]+tc.name, func(t *testing.T) {
				ctx := context.Background()
				g, task, input, source, downloads := newTestDirectHfSource(t)
				defer g.taskQueue.close()
				path := input.Parent.LocalPath
				if direct {
					task.BaseModel.Spec.Storage.DownloadPolicy = nil
					path = input.ChildModelPath
					require.NoError(t, os.MkdirAll(path, 0700))
					require.NoError(t, os.WriteFile(filepath.Join(path, "config.json"), []byte("{}"), 0600))
					weights := "damaged"
					if tc.healthy {
						weights = "weights"
					}
					require.NoError(t, os.WriteFile(filepath.Join(path, "model.safetensors"), []byte(weights), 0600))
				} else {
					require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
				}
				task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-2"
				g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
				node := tc.node
				if node == "local" {
					node = g.configMapReconciler.nodeName
				}
				_, err := g.kubeClient.CoreV1().Pods("serving").Create(ctx, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "consumer"},
					Spec: corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{Path: path},
					}}}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
				if direct {
					_, err = source.process(ctx, g, task, task.BaseModel.Spec, true)
				} else {
					_, err = g.runHfArtifactDownload(ctx, task, input, true,
						func(string) (bool, error) { return tc.healthy, nil },
						func(string) error { *downloads++; return nil })
				}
				if tc.blocked {
					require.Error(t, err)
					require.Zero(t, *downloads)
					if direct {
						data, readErr := os.ReadFile(filepath.Join(path, "model.safetensors"))
						require.NoError(t, readErr)
						require.Equal(t, "damaged", string(data))
					}
				} else {
					require.NoError(t, err)
					if tc.healthy {
						require.Zero(t, *downloads)
					} else {
						require.Equal(t, 1, *downloads)
					}
				}
			})
		}
	}
}
