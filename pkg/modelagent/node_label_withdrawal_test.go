package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func newArtifactWithdrawalTest(t *testing.T, kind string) (*Gopher, *GopherTask, string, func() error) {
	t.Helper()
	ctx := context.Background()
	var g *Gopher
	var task *GopherTask
	var path string
	var withdraw func() error
	if kind == "shared eviction" {
		var input hfArtifactTaskInput
		g, task, input = newSharedEvictionTestModel(t)
		path = input.ChildModelPath
		g.guardSharedEvictionCleanup(task, &input)
		withdraw = func() error { return input.prepareDeletionPhase(ctx) }
	} else {
		g, task, path = newEvictionTestModel(t)
		withdraw = func() error { return g.evictDirectArtifact(ctx, task) }
		if kind == "restoration" {
			task.TaskType = Download
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			withdraw = func() error { return g.withdrawRestorationReadiness(ctx, task) }
		}
	}
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	node.UID, node.ResourceVersion = "node-uid", "1"
	node.Labels[constants.GetModelArtifactRequestLabel(task.BaseModel.UID)] = "request"
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	g.nodeLabelReconciler.nodeUID = node.UID
	return g, task, path, withdraw
}

func TestArtifactWithdrawalReturnsPatchErrors(t *testing.T) {
	for _, kind := range []string{"direct eviction", "shared eviction", "restoration"} {
		for _, failure := range []string{"not found", "bad request", "unavailable", "ineffective"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				g, task, path, withdraw := newArtifactWithdrawalTest(t, kind)
				var patchErr error
				switch failure {
				case "not found":
					patchErr = apierrors.NewNotFound(corev1.Resource("nodes"), g.configMapReconciler.nodeName)
				case "bad request":
					patchErr = apierrors.NewBadRequest("invalid patch")
				case "unavailable":
					patchErr = apierrors.NewServiceUnavailable("node API unavailable")
				}
				client := g.kubeClient.(*k8sfake.Clientset)
				client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					if patchErr != nil {
						return true, nil, patchErr
					}
					obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", g.configMapReconciler.nodeName)
					return true, obj, err
				})
				err := withdraw()
				if patchErr != nil {
					require.ErrorIs(t, err, patchErr, "strict withdrawal must return the actual patch failure")
				} else {
					require.Error(t, err, "a successful response is insufficient when Ready remains")
				}
				_, err = os.Lstat(path)
				require.NoError(t, err)
				node, err := client.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, "Ready", node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
			})
		}
	}
}

func TestArtifactWithdrawalRetriesIneffectivePatch(t *testing.T) {
	for _, kind := range []string{"direct eviction", "shared eviction", "restoration"} {
		t.Run(kind, func(t *testing.T) {
			g, task, _, withdraw := newArtifactWithdrawalTest(t, kind)
			g.nodeLabelReconciler.opRetry = 2
			client := g.kubeClient.(*k8sfake.Clientset)
			patches := 0
			client.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
				patches++
				var payload []patchStringValue
				require.NoError(t, json.Unmarshal(action.(k8stesting.PatchAction).GetPatch(), &payload))
				require.Contains(t, payload, patchStringValue{Op: "test", Path: "/metadata/uid", Value: "node-uid"})
				require.Contains(t, payload, patchStringValue{Op: "test", Path: "/metadata/resourceVersion", Value: fmt.Sprint(patches)})
				if patches == 1 {
					obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", g.configMapReconciler.nodeName)
					require.NoError(t, err)
					node := obj.(*corev1.Node).DeepCopy()
					node.ResourceVersion = "2"
					node.Labels["unrelated"] = "preserved"
					require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
					return true, node, nil
				}
				return false, nil, nil
			})
			require.NoError(t, withdraw())
			require.Equal(t, 2, patches)
			node, err := client.CoreV1().Nodes().Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			label := constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)
			require.Equal(t, "preserved", node.Labels["unrelated"])
			if kind == "restoration" {
				require.NotEqual(t, "Ready", node.Labels[label])
			} else {
				require.NotContains(t, node.Labels, label)
				require.NotContains(t, node.Labels, constants.GetModelArtifactRequestLabel(task.BaseModel.UID))
			}
		})
	}
}

func TestArtifactWithdrawalRechecksIntentAndNode(t *testing.T) {
	for _, kind := range []string{"direct eviction", "shared eviction", "restoration"} {
		for _, change := range []string{"intent during retry", "node during retry", "node during retry unbound", "node after patch"} {
			t.Run(kind+"/"+change, func(t *testing.T) {
				g, task, path, withdraw := newArtifactWithdrawalTest(t, kind)
				g.nodeLabelReconciler.opRetry = 2
				if change == "node during retry unbound" {
					g.nodeLabelReconciler.nodeUID = ""
				}
				client := g.kubeClient.(*k8sfake.Clientset)
				patches := 0
				client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					patches++
					if patches > 1 {
						return false, nil, nil
					}
					if change == "intent during retry" {
						latest := task.BaseModel.DeepCopy()
						latest.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "new-request"
						require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("basemodels"), latest, latest.Namespace))
					} else {
						obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", g.configMapReconciler.nodeName)
						require.NoError(t, err)
						node := obj.(*corev1.Node).DeepCopy()
						node.UID = "replacement-node"
						if change == "node after patch" {
							delete(node.Labels, constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name))
						}
						require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
						if change == "node after patch" {
							return true, node, nil
						}
					}
					return true, nil, apierrors.NewConflict(corev1.Resource("nodes"), g.configMapReconciler.nodeName, fmt.Errorf("newer publication"))
				})
				require.Error(t, withdraw())
				require.Equal(t, 1, patches, "retry must revalidate before another patch")
				_, err := os.Lstat(path)
				require.NoError(t, err, "failed withdrawal cannot authorize cleanup")
			})
		}
	}
}
