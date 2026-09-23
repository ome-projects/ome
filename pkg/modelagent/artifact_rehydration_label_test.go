package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestRehydrationUpdatingLabelFailureStopsWrites(t *testing.T) {
	for _, taskType := range []GopherTaskType{Download, DownloadOverride} {
		for _, mode := range []string{"request", "residency", "ordinary"} {
			for _, fault := range []string{"get", "patch", "canceled patch"} {
				t.Run(string(taskType)+"/"+mode+"/"+fault, func(t *testing.T) {
					root := t.TempDir()
					path := filepath.Join(root, "model")
					require.NoError(t, os.Mkdir(path, 0700))
					weights := filepath.Join(path, "weights")
					require.NoError(t, os.WriteFile(weights, []byte("existing weights"), 0600))
					model := &v1beta1.BaseModel{
						ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "model-uid"},
						Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
							StorageUri: stringPtr("oci://n/ns/b/bucket/o/model"), Path: &path,
							// Ordinary work must reach client construction without network access.
							Parameters: &map[string]string{"auth": "unsupported-test-auth"},
						}},
					}
					request := ""
					if mode == "request" {
						request = "restore-1"
						model.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: request}
					}
					key := getModelID(model, nil)
					raw, err := json.Marshal(ModelEntry{Name: model.Name, ModelUID: model.UID, Status: ModelStatusReady, ArtifactRehydrationID: request})
					require.NoError(t, err)
					labels := map[string]string{constants.GetBaseModelLabel(model.Namespace, model.Name): "Ready"}
					if request != "" {
						requestKey, err := constants.ArtifactReadyLabelKey(model.UID)
						require.NoError(t, err)
						labels[requestKey] = request
					}
					cm := makeConfigMap("node1", map[string]string{key: string(raw)})
					cm.Annotations = map[string]string{constants.ModelArtifactNodeUIDAnnotation: "node-uid"}
					g := newGopherForProcessTask(cm, labels)
					g.modelRootDir, g.kubeClient = root, g.configMapReconciler.kubeClient
					g.modelClient = omefake.NewSimpleClientset(model)
					g.metrics, g.downloadRetry = NewMetrics(prometheus.NewRegistry()), 1
					initializeArtifactEvictionTestNode(t, g)
					task := &GopherTask{TaskType: taskType, BaseModel: model, ResidencyManaged: mode == "residency"}
					client := g.kubeClient.(*k8sfake.Clientset)
					client.ClearActions()
					verb := "patch"
					if fault == "get" {
						verb = "get"
					}
					labelCalls := 0
					client.PrependReactor(verb, "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
						labelCalls++
						if fault == "canceled patch" {
							if task.ResidencyManaged {
								attempt, outcome := g.taskTracker.beginDelete(gopherTaskModelKey(task), g.taskTracker.ensureSequence(0))
								require.Equal(t, gopherTaskWait, outcome)
								t.Cleanup(func() { g.taskTracker.finishDelete(attempt, false) })
							} else {
								g.taskTracker.cancelLegacyDownload(gopherTaskModelKey(task))
							}
						}
						return true, nil, errors.New("label API unavailable")
					})

					err = g.processTask(task)
					require.Positive(t, labelCalls, "must reach the injected label failure")
					if fault == "canceled patch" {
						require.ErrorIs(t, err, context.Canceled)
					} else if mode == "ordinary" {
						require.ErrorContains(t, err, "failed to create object storage client", "ordinary no-request behavior must remain unchanged")
						require.DirExists(t, filepath.Join(root, hfArtifactLockDirectory), "ordinary work still reaches the direct writer")
						return
					} else {
						require.ErrorContains(t, err, "label API unavailable")
					}
					entries, err := os.ReadDir(root)
					require.NoError(t, err)
					require.Len(t, entries, 1, "must not create writer locks or staging directories")
					entries, err = os.ReadDir(path)
					require.NoError(t, err)
					require.Len(t, entries, 1, "must not create model files")
					contents, err := os.ReadFile(weights)
					require.NoError(t, err)
					require.Equal(t, "existing weights", string(contents))
					for _, action := range client.Actions() {
						if action.GetResource().Resource == "configmaps" {
							require.Equal(t, "get", action.GetVerb(), "failed withdrawal must not write acknowledgement state")
						}
					}
					persisted, err := g.configMapReconciler.getConfigMap(context.Background())
					require.NoError(t, err)
					require.Equal(t, cm.Data, persisted.Data)
				})
			}
		}
	}
}
