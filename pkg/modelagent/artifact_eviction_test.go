package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestArtifactEvictionNodeLabel(t *testing.T) {
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "old-model", Namespace: "customer"}}
	label := constants.GetBaseModelLabel(model.Namespace, model.Name)
	c := k8sfake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{label: "Ready"}}})
	r := NewNodeLabelReconciler("node1", c, 1, zap.NewNop().Sugar())
	require.NoError(t, r.ReconcileNodeLabels(&NodeLabelOp{BaseModel: model, ModelStateOnNode: "Evicted"}))
	node, err := c.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Evicted", node.Labels[label])
}

func TestArtifactEvictionProtectsConsumersAndFailsClosed(t *testing.T) {
	for _, scenario := range []string{"pending-service", "terminating-service", "pod-on-other-node", "reserved", "other-model-path", "api-unavailable", "consumer-at-second-check"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "model")
			require.NoError(t, os.Mkdir(path, 0700))
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "customer", UID: "uid", Annotations: map[string]string{"ome.io/artifact-residency": "Evicted"}}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model"), Path: &path}}}
			if scenario == "reserved" {
				model.Labels = map[string]string{constants.ReserveModelArtifact: "true"}
			}
			key := constants.GetModelConfigMapKey(model.Namespace, model.Name, false)
			label := constants.GetBaseModelLabel(model.Namespace, model.Name)
			g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{key: modelEntryJSON(ModelStatusReady)}), map[string]string{label: "Ready"})
			g.modelRootDir, g.kubeClient = root, g.configMapReconciler.kubeClient
			c := omefake.NewSimpleClientset(model)
			g.modelClient = c
			switch scenario {
			case "pending-service", "terminating-service":
				service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: model.Namespace}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: model.Name}}}
				if scenario == "terminating-service" {
					now := metav1.Now()
					service.DeletionTimestamp = &now
				}
				_, err := c.OmeV1beta1().InferenceServices(model.Namespace).Create(context.Background(), service, metav1.CreateOptions{})
				require.NoError(t, err)
			case "pod-on-other-node":
				_, err := g.kubeClient.CoreV1().Pods("other").Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orphan"}, Spec: corev1.PodSpec{NodeName: "node2", NodeSelector: map[string]string{label: "Ready"}}}, metav1.CreateOptions{})
				require.NoError(t, err)
			case "other-model-path":
				other := model.DeepCopy()
				other.Name, other.UID = "other", "other-uid"
				_, err := c.OmeV1beta1().BaseModels(model.Namespace).Create(context.Background(), other, metav1.CreateOptions{})
				require.NoError(t, err)
			case "api-unavailable":
				c.PrependReactor("list", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, fmt.Errorf("unavailable") })
			case "consumer-at-second-check":
				checks := 0
				c.PrependReactor("list", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
					checks++
					if checks == 2 {
						return true, &v1beta1.InferenceServiceList{Items: []v1beta1.InferenceService{{ObjectMeta: metav1.ObjectMeta{Namespace: model.Namespace}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: model.Name}}}}}, nil
					}
					return false, nil, nil
				})
			}
			_, _ = g.processArtifactEviction(context.Background(), &GopherTask{TaskType: Evict, BaseModel: model})
			require.DirExists(t, path)
			node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, "Ready", node.Labels[label])
		})
	}
}

func TestArtifactEvictionPreservesSiblingAndChildUID(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	handler := g.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	second := input
	second.ChildModelKey, second.ChildModelUID = "default.basemodel.model-2", "uid-2"
	second.ChildModelPath = filepath.Join(input.ModelStoreRoot, "model-2")
	seedTestChildModelEntry(t, handler.repository, second)
	require.NoError(t, runTestHfArtifactDownload(handler, second))
	task.TaskType = Evict
	task.BaseModel.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	g.kubeClient = g.configMapReconciler.kubeClient
	label := constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)
	_, err := g.kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: g.configMapReconciler.nodeName, Labels: map[string]string{label: "Ready"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
	_, err = g.kubeClient.CoreV1().Pods("other").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sibling-consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: second.ChildModelPath},
		}}}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	waiting, err := g.processArtifactEviction(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	assertChildPathMissing(t, input.ChildModelPath)
	require.DirExists(t, input.Parent.LocalPath)
	require.True(t, handler.files.IsChildLinkedToParent(second.ChildModelPath, input.Parent.LocalPath))
	pending, err := handler.repository.pendingDeletion(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.Nil(t, pending, "completed eviction must not retain a deletion receipt")
	// Eviction must not invalidate this still-existing CR UID like CR deletion.
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	require.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
}

func TestArtifactEvictionProtectsStoredArtifactPaths(t *testing.T) {
	for _, scenario := range []string{"changed-child-path", "pending-old-child", "outside-parent-root", "parent-pod", "parent-model", "child-subdir-model", "child-subdir-cluster", "child-subdir-pod", "child-subdir-subpath"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			g, task, input := newTestHfArtifactGopher(t)
			handler := g.sharedHfArtifactHandler()
			require.NoError(t, runTestHfArtifactDownload(handler, input))
			task.TaskType = Evict
			task.BaseModel.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
			g.kubeClient = g.configMapReconciler.kubeClient
			label := constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)
			_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: g.configMapReconciler.nodeName, Labels: map[string]string{label: "Ready"},
			}}, metav1.CreateOptions{})
			require.NoError(t, err)
			g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
			switch scenario {
			case "changed-child-path", "pending-old-child":
				task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(input.ModelStoreRoot, "new-path"))
				if scenario == "pending-old-child" {
					_, err := handler.repository.removeModelReference(ctx, input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
					require.NoError(t, err)
				}
			case "outside-parent-root":
				parentPath := canonicalHfArtifactPath(filepath.Join(t.TempDir(), "child"), input.Parent.Identity)
				require.NoError(t, os.MkdirAll(filepath.Dir(parentPath), 0700))
				require.NoError(t, os.Rename(input.Parent.LocalPath, parentPath))
				require.NoError(t, os.Remove(input.ChildModelPath))
				require.NoError(t, os.Symlink(parentPath, input.ChildModelPath))
				cm, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				parent, err := decodeHfArtifactEntry(input.Parent.Key, cm.Data[input.Parent.Key])
				require.NoError(t, err)
				parent.LocalPath = parentPath
				raw, err := json.Marshal(parent)
				require.NoError(t, err)
				cm.Data[parent.Key] = string(raw)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
				input.Parent = parent
			case "parent-pod":
				_, err := g.kubeClient.CoreV1().Pods("other").Create(ctx, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "parent-consumer"},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{Path: input.Parent.LocalPath},
					}}}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			if scenario == "parent-model" {
				other := task.BaseModel.DeepCopy()
				other.Name, other.UID = "parent-consumer", "other-uid"
				other.Spec.Storage.Path = stringPtr(input.Parent.LocalPath)
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			if strings.HasPrefix(scenario, "child-subdir-") {
				require.NoError(t, os.Mkdir(filepath.Join(input.Parent.LocalPath, "submodel"), 0700))
				reference := filepath.Join(input.ChildModelPath, "submodel")
				store := &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + reference)}
				switch scenario {
				case "child-subdir-model":
					_, err = g.modelClient.OmeV1beta1().BaseModels("other").Create(ctx, &v1beta1.BaseModel{
						ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "other", UID: "consumer"},
						Spec:       v1beta1.BaseModelSpec{Storage: store},
					}, metav1.CreateOptions{})
				case "child-subdir-cluster":
					_, err = g.modelClient.OmeV1beta1().ClusterBaseModels().Create(ctx, &v1beta1.ClusterBaseModel{
						ObjectMeta: metav1.ObjectMeta{Name: "consumer", UID: "consumer"},
						Spec:       v1beta1.BaseModelSpec{Storage: store},
					}, metav1.CreateOptions{})
				default:
					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer"}, Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: reference}}}},
					}}
					if scenario == "child-subdir-subpath" {
						pod.Spec.Volumes[0].HostPath.Path = input.ModelStoreRoot
						pod.Spec.Containers = []corev1.Container{{Name: "server", VolumeMounts: []corev1.VolumeMount{{Name: "model", MountPath: "/model", SubPath: filepath.Join(filepath.Base(input.ChildModelPath), "submodel")}}}}
					}
					_, err = g.kubeClient.CoreV1().Pods("other").Create(ctx, pod, metav1.CreateOptions{})
				}
				require.NoError(t, err)
			}
			before, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			waiting, evictionErr := g.processArtifactEviction(ctx, task)
			require.FileExists(t, filepath.Join(input.Parent.LocalPath, "config.json"))
			after, err := g.configMapReconciler.getConfigMap(ctx)
			require.NoError(t, err)
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			if scenario == "parent-pod" || scenario == "parent-model" {
				require.NoError(t, evictionErr)
				require.False(t, waiting)
				assertChildPathMissing(t, input.ChildModelPath)
				require.Equal(t, "Evicted", node.Labels[label])
				assertParentEntryReady(t, handler.repository, input.Parent, nil)
			} else {
				require.True(t, handler.files.IsChildLinkedToParent(input.ChildModelPath, input.Parent.LocalPath))
				require.Equal(t, before.Data, after.Data, "rejected eviction must preserve persisted ownership")
				require.Equal(t, "Ready", node.Labels[label])
			}
		})
	}
}

func TestArtifactEvictionRejectsSymlinkTraversalBeforeLocalChanges(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, activeDownload := range []bool{false, true} {
			t.Run(fmt.Sprintf("cluster=%v/active=%v", cluster, activeDownload), func(t *testing.T) {
				ctx := context.Background()
				g, task, cleanedPath := newArtifactEvictionTestModel(t)
				actualPath := filepath.Join(g.modelRootDir, "actual", "model")
				linkTarget := filepath.Join(g.modelRootDir, "actual", "subdir")
				require.NoError(t, os.MkdirAll(actualPath, 0700))
				require.NoError(t, os.MkdirAll(linkTarget, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(actualPath, "weights"), []byte("actual weights"), 0600))
				require.NoError(t, os.Symlink(linkTarget, filepath.Join(g.modelRootDir, "link")))
				rawPath := g.modelRootDir + "/link/../model"
				resolved, err := filepath.EvalSymlinks(rawPath)
				require.NoError(t, err)
				expected, err := filepath.EvalSymlinks(actualPath)
				require.NoError(t, err)
				require.Equal(t, expected, resolved)
				require.Equal(t, cleanedPath, filepath.Clean(rawPath), "cleaning must demonstrate the different deletion target")
				task.BaseModel.Spec.Storage.Path = &rawPath
				if cluster {
					task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: *task.BaseModel.ObjectMeta.DeepCopy(), Spec: task.BaseModel.Spec}
					task.ClusterBaseModel.Namespace = ""
					task.BaseModel = nil
					g.modelClient = omefake.NewSimpleClientset(task.ClusterBaseModel)
				} else {
					g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
				}
				downloadCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				if activeDownload {
					attempt := g.taskTracker.beginLegacyTask(gopherTaskModelKey(task), cancel)
					defer g.taskTracker.finishLegacyTask(attempt)
				}
				before, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				nodeBefore, err := g.kubeClient.CoreV1().Nodes().Get(ctx, before.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.ErrorContains(t, g.processTask(task), "canonical absolute artifact path")
				require.NoError(t, downloadCtx.Err(), "invalid eviction must not cancel an active download")
				require.FileExists(t, filepath.Join(cleanedPath, "weights"))
				require.FileExists(t, filepath.Join(actualPath, "weights"))
				after, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				require.Equal(t, before.Data, after.Data)
				nodeAfter, err := g.kubeClient.CoreV1().Nodes().Get(ctx, before.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, nodeBefore.Labels, nodeAfter.Labels)
			})
		}
	}
}

func TestArtifactEvictionPathBoundary(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "alias")))
	for _, path := range []string{root, outside, filepath.Join(root, "_artifacts", "parent"), filepath.Join(root, "alias", "model")} {
		_, err := safeArtifactEvictionPath(root, path)
		require.Error(t, err, path)
	}
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "child")))
	_, err := safeArtifactEvictionPath(root, filepath.Join(root, "child"))
	require.NoError(t, err, "unlinking a child symlink must not follow its target")
}

func TestArtifactEvictionRejectsReservedPathsAndSymlinkedRoot(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{filepath.Join(root, hfArtifactLockDirectory), filepath.Join(root, hfArtifactLockDirectory, "owner.lock")} {
		_, err := safeArtifactEvictionPath(root, path)
		require.Error(t, err, "eviction must not remove stable lock inodes")
	}
	alias := filepath.Join(t.TempDir(), "root")
	require.NoError(t, os.Symlink(root, alias))
	_, err := safeArtifactEvictionPath(alias, filepath.Join(alias, "model"))
	require.Error(t, err, "a symlink must not be accepted as the configured store root")
}

func newArtifactEvictionTestModel(t *testing.T) (*Gopher, *GopherTask, string) {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "model")
	require.NoError(t, os.Mkdir(path, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("weights"), 0600))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "customer", UID: "uid", Annotations: map[string]string{constants.ModelArtifactResidencyAnnotation: constants.ModelArtifactResidencyEvicted}}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("hf://org/model"), Path: &path}}}
	key := getModelID(model, nil)
	raw, err := json.Marshal(ModelEntry{Name: model.Name, ModelUID: model.UID, Status: ModelStatusReady})
	require.NoError(t, err)
	g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{key: string(raw)}), map[string]string{constants.GetBaseModelLabel(model.Namespace, model.Name): "Ready"})
	g.modelRootDir = root
	g.modelClient = omefake.NewSimpleClientset(model)
	g.kubeClient = g.configMapReconciler.kubeClient
	g.artifactRouting.known = true
	initializeArtifactEvictionTestNode(t, g)
	return g, &GopherTask{TaskType: Evict, BaseModel: model}, path
}

func initializeArtifactEvictionTestNode(t *testing.T, g *Gopher) {
	t.Helper()
	ctx := context.Background()
	node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	// The fake client does not populate API-server identity fields.
	node.UID, node.ResourceVersion = "node-uid", "1"
	_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func TestArtifactEvictionProtectsLocalURIConsumers(t *testing.T) {
	for _, shared := range []bool{false, true} {
		for _, cluster := range []bool{false, true} {
			for _, emptyPath := range []bool{false, true} {
				t.Run(fmt.Sprintf("shared=%v/cluster=%v/emptyPath=%v", shared, cluster, emptyPath), func(t *testing.T) {
					ctx := context.Background()
					var g *Gopher
					var task *GopherTask
					var protectedPath, protectedFile string
					if shared {
						var input hfArtifactTaskInput
						g, task, input = newTestHfArtifactGopher(t)
						require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
						task.TaskType = Evict
						task.BaseModel.Annotations[constants.ModelArtifactResidencyAnnotation] = constants.ModelArtifactResidencyEvicted
						g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
						g.kubeClient = g.configMapReconciler.kubeClient
						label := constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)
						_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
							Name: g.configMapReconciler.nodeName, Labels: map[string]string{label: "Ready"},
						}}, metav1.CreateOptions{})
						require.NoError(t, err)
						g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
						protectedPath, protectedFile = input.Parent.LocalPath, "config.json"
					} else {
						g, task, protectedPath = newArtifactEvictionTestModel(t)
						protectedFile = "weights"
					}
					store := &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + protectedPath)}
					if emptyPath {
						store.Path = stringPtr("")
					}
					meta := metav1.ObjectMeta{Name: "local-consumer", UID: "local-consumer-uid"}
					if cluster {
						_, err := g.modelClient.OmeV1beta1().ClusterBaseModels().Create(ctx, &v1beta1.ClusterBaseModel{
							ObjectMeta: meta, Spec: v1beta1.BaseModelSpec{Storage: store},
						}, metav1.CreateOptions{})
						require.NoError(t, err)
					} else {
						meta.Namespace = "other"
						_, err := g.modelClient.OmeV1beta1().BaseModels(meta.Namespace).Create(ctx, &v1beta1.BaseModel{
							ObjectMeta: meta, Spec: v1beta1.BaseModelSpec{Storage: store},
						}, metav1.CreateOptions{})
						require.NoError(t, err)
					}
					waiting, err := g.processArtifactEviction(ctx, task)
					require.FileExists(t, filepath.Join(protectedPath, protectedFile))
					if shared {
						require.NoError(t, err)
						require.False(t, waiting)
						assertChildPathMissing(t, *task.BaseModel.Spec.Storage.Path)
					} else {
						// This fixture has no retry budget; the preflight rejection
						// is returned instead of starting a background retry timer.
						require.ErrorContains(t, err, "model path is used by another Model CR")
						node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
						require.NoError(t, err)
						require.Equal(t, "Ready", node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
					}
				})
			}
		}
	}
}

func TestModelPathForEvictionProtection(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct {
		name    string
		storage *v1beta1.StorageSpec
		want    string
		wantErr bool
	}{
		{name: "no-storage"},
		{name: "no-path-or-uri", storage: &v1beta1.StorageSpec{}},
		{name: "explicit-path", storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local:///ignored"), Path: &root}, want: root},
		{name: "path-without-uri", storage: &v1beta1.StorageSpec{Path: &root}, want: root},
		{name: "invalid-local-uri", storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://")}, wantErr: true},
		{name: "ordinary-fallback", storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model"), Path: stringPtr("")}, want: root + "/oci://n/ns/b/bucket/o/model"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, err := modelPathForEvictionProtection(&v1beta1.BaseModelSpec{Storage: tc.storage}, root)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, path)
		})
	}
}

func TestPathReferencesArtifact(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	parent := filepath.Join(root, "parent")
	child := filepath.Join(root, "child")
	sibling := filepath.Join(root, "sibling")
	external := filepath.Join(t.TempDir(), "external")
	cycle := filepath.Join(root, "cycle")
	require.NoError(t, os.MkdirAll(filepath.Join(parent, "subdir"), 0700))
	require.NoError(t, os.Symlink("parent", child))
	require.NoError(t, os.Symlink("parent", sibling))
	require.NoError(t, os.Symlink(child, external))
	require.NoError(t, os.Symlink("cycle", cycle))
	for _, tc := range []struct {
		name, reference, target string
		allowAncestor, want     bool
		wantErr                 bool
	}{
		{name: "direct-child", reference: child, target: child, want: true},
		{name: "child-subdir", reference: filepath.Join(child, "subdir"), target: child, want: true},
		{name: "absent-subdir", reference: filepath.Join(child, "absent"), target: child, want: true},
		{name: "external-to-child", reference: external, target: child, want: true},
		{name: "external-to-parent", reference: external, target: parent, want: true},
		{name: "independent-sibling", reference: sibling, target: child, allowAncestor: true},
		{name: "sibling-protects-parent", reference: sibling, target: parent, want: true},
		{name: "broad-pod-mount", reference: root, target: child},
		{name: "containing-model-path", reference: root, target: child, allowAncestor: true, want: true},
		{name: "filesystem-root-pod", reference: "/", target: child},
		{name: "filesystem-root-model", reference: "/", target: child, allowAncestor: true, want: true},
		{name: "unrelated", reference: filepath.Join(root, "other"), target: child, allowAncestor: true},
		{name: "cycle", reference: cycle, target: child, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uses, err := pathReferencesArtifact(tc.reference, tc.target, tc.allowAncestor)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, uses)
		})
	}
}

func TestArtifactEvictionPreservesLegacyParent(t *testing.T) {
	for _, recorded := range []bool{false, true} {
		t.Run(fmt.Sprintf("recorded=%v", recorded), func(t *testing.T) {
			g, task, path := newArtifactEvictionTestModel(t)
			child := filepath.Join(g.modelRootDir, "child")
			if recorded {
				cm, err := g.configMapReconciler.getConfigMap(context.Background())
				require.NoError(t, err)
				raw, err := json.Marshal(ModelEntry{Name: task.BaseModel.Name, Status: ModelStatusReady, Config: &ModelConfig{Artifact: Artifact{ChildrenPaths: []string{child}}}})
				require.NoError(t, err)
				cm.Data[getModelID(task.BaseModel, nil)] = string(raw)
				_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			} else {
				require.NoError(t, os.Symlink(path, child))
			}
			_, err := g.processArtifactEviction(context.Background(), task)
			require.Error(t, err)
			require.FileExists(t, filepath.Join(path, "weights"))
			node, err := g.kubeClient.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, "Ready", node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
		})
	}
}

func TestRejectedArtifactEvictionDoesNotCancelDownload(t *testing.T) {
	g, eviction, path := newArtifactEvictionTestModel(t)
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: eviction.BaseModel.Namespace}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: eviction.BaseModel.Name}}}
	_, err := g.modelClient.OmeV1beta1().InferenceServices(service.Namespace).Create(context.Background(), service, metav1.CreateOptions{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	active := g.taskTracker.beginLegacyTask(gopherTaskModelKey(eviction), cancel)
	defer g.taskTracker.finishLegacyTask(active)
	g.enqueueTask(eviction)
	require.NoError(t, ctx.Err(), "queue admission must not cancel work before eviction checks")
	require.NoError(t, g.processTask(eviction))
	require.NoError(t, ctx.Err(), "a protected model's download must continue")
	require.FileExists(t, filepath.Join(path, "weights"))
}

func TestWithdrawnArtifactEvictionReleasesHydrationBarrier(t *testing.T) {
	g, eviction, path := newArtifactEvictionTestModel(t)
	eviction.Sequence = g.taskTracker.ensureSequence(0)
	attempt, outcome := g.taskTracker.beginDelete(gopherTaskModelKey(eviction), eviction.Sequence)
	require.Equal(t, gopherTaskProceed, outcome)
	g.taskTracker.finishDelete(attempt, true)
	model := eviction.BaseModel.DeepCopy()
	delete(model.Annotations, constants.ModelArtifactResidencyAnnotation)
	_, err := g.modelClient.OmeV1beta1().BaseModels(model.Namespace).Update(context.Background(), model, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.processTask(eviction))
	download, outcome := g.taskTracker.beginDownload(gopherTaskModelKey(eviction), g.taskTracker.ensureSequence(0), nil)
	require.Equal(t, gopherTaskProceed, outcome)
	g.taskTracker.finishDownload(download)
	require.FileExists(t, filepath.Join(path, "weights"))
}

func TestArtifactEvictionWaitsForCancelledDownloadToFinish(t *testing.T) {
	g, eviction, path := newArtifactEvictionTestModel(t)
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	active := g.taskTracker.beginLegacyTask(gopherTaskModelKey(eviction), cancel)
	g.enqueueTask(eviction)
	require.NoError(t, g.processTask(eviction))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.FileExists(t, filepath.Join(path, "weights"), "cleanup must wait for the writer to finish")
	g.taskTracker.finishLegacyTask(active)
	select {
	case retry := <-g.gopherChan:
		require.NoError(t, g.processTask(retry))
	case <-time.After(time.Second):
		t.Fatal("eviction was not requeued")
	}
	require.NoDirExists(t, path)
}

func TestArtifactEvictionClearsDownloadProgress(t *testing.T) {
	g, task, _ := newArtifactEvictionTestModel(t)
	cm, err := g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	key := getModelID(task.BaseModel, nil)
	raw, err := json.Marshal(ModelEntry{Name: task.BaseModel.Name, ModelUID: task.BaseModel.UID, Status: ModelStatusUpdating, Progress: &DownloadProgress{}})
	require.NoError(t, err)
	cm.Data[key] = string(raw)
	_, err = g.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, g.processTask(task))
	cm, err = g.configMapReconciler.getConfigMap(context.Background())
	require.NoError(t, err)
	var entry ModelEntry
	require.NoError(t, json.Unmarshal([]byte(cm.Data[key]), &entry))
	require.Equal(t, ModelStatusEvicted, entry.Status)
	require.Nil(t, entry.Progress, "an evicted artifact must not retain an in-progress transfer")
}

func TestArtifactEvictionRejectsShardedModels(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprintf("cluster=%v", cluster), func(t *testing.T) {
			g, task, path := newArtifactEvictionTestModel(t)
			distribution := v1beta1.DistributionSharded
			task.BaseModel.Spec.Distribution = &distribution
			if cluster {
				task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: *task.BaseModel.ObjectMeta.DeepCopy(), Spec: task.BaseModel.Spec}
				task.ClusterBaseModel.Namespace = ""
				task.BaseModel = nil
				g.modelClient = omefake.NewSimpleClientset(task.ClusterBaseModel)
			} else {
				g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			}
			_, err := g.processArtifactEviction(context.Background(), task)
			require.ErrorContains(t, err, "PerNode")
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestArtifactEvictionRejectsMissingChildPath(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			path *string
		}{
			{name: "nil"},
			{name: "empty", path: stringPtr("")},
			{name: "whitespace", path: stringPtr(" \t ")},
		} {
			t.Run(fmt.Sprintf("cluster=%v/%s", cluster, tc.name), func(t *testing.T) {
				ctx := context.Background()
				g, task, originalPath := newArtifactEvictionTestModel(t)
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
				task.BaseModel.Spec.Storage.Path = stringPtr("")
				fallback := getDestPath(&task.BaseModel.Spec, g.modelRootDir)
				require.NoError(t, os.MkdirAll(fallback, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(fallback, "weights"), []byte("weights"), 0600))
				task.BaseModel.Spec.Storage.Path = tc.path
				if cluster {
					task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: *task.BaseModel.ObjectMeta.DeepCopy(), Spec: task.BaseModel.Spec}
					task.ClusterBaseModel.Namespace = ""
					task.BaseModel = nil
					g.modelClient = omefake.NewSimpleClientset(task.ClusterBaseModel)
				} else {
					g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
				}
				before, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				nodeBefore, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				_, err = g.processArtifactEviction(ctx, task)
				require.ErrorContains(t, err, "explicit model storage URI and child path")
				require.FileExists(t, filepath.Join(originalPath, "weights"))
				require.FileExists(t, filepath.Join(fallback, "weights"), "an unsupported request must not evict the default download directory")
				after, err := g.configMapReconciler.getConfigMap(ctx)
				require.NoError(t, err)
				require.Equal(t, before.Data, after.Data)
				nodeAfter, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, nodeBefore.Labels, nodeAfter.Labels)
			})
		}
	}
}

func TestArtifactEvictionProtectsAliasedPathReferences(t *testing.T) {
	for _, consumer := range []string{"model", "model-leaf", "pod", "pod-leaf"} {
		t.Run(consumer, func(t *testing.T) {
			g, task, path := newArtifactEvictionTestModel(t)
			alias := filepath.Join(t.TempDir(), "store")
			require.NoError(t, os.Symlink(g.modelRootDir, alias))
			aliasPath := filepath.Join(alias, "model")
			if consumer == "pod-leaf" || consumer == "model-leaf" {
				aliasPath = filepath.Join(t.TempDir(), "mounted-model")
				require.NoError(t, os.Symlink(path, aliasPath))
			}
			if consumer == "model" || consumer == "model-leaf" {
				other := task.BaseModel.DeepCopy()
				other.Name, other.UID = "other", "other-uid"
				other.Spec.Storage.Path = &aliasPath
				other.Spec.Storage.StorageUri = stringPtr("local://" + aliasPath)
				_, err := g.modelClient.OmeV1beta1().BaseModels(other.Namespace).Create(context.Background(), other, metav1.CreateOptions{})
				require.NoError(t, err)
			} else {
				_, err := g.kubeClient.CoreV1().Pods("customer").Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "serving"}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: aliasPath}}}}}}, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			_, _ = g.processArtifactEviction(context.Background(), task)
			require.FileExists(t, filepath.Join(path, "weights"))
		})
	}
}

func TestArtifactEvictionProtectsDynamicPodSubpaths(t *testing.T) {
	for _, overlapping := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlapping=%v", overlapping), func(t *testing.T) {
			g, task, path := newArtifactEvictionTestModel(t)
			mountRoot := t.TempDir()
			if overlapping {
				mountRoot = g.modelRootDir
			}
			_, err := g.kubeClient.CoreV1().Pods("customer").Create(context.Background(), &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "serving"},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{Name: "models", VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{Path: mountRoot},
					}}},
					Containers: []corev1.Container{{Name: "server",
						Env:          []corev1.EnvVar{{Name: "MODEL", Value: "model"}},
						VolumeMounts: []corev1.VolumeMount{{Name: "models", MountPath: "/model", SubPathExpr: "$(MODEL)"}},
					}},
				},
			}, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = g.processArtifactEviction(context.Background(), task)
			require.NoError(t, err)
			if overlapping {
				require.FileExists(t, filepath.Join(path, "weights"))
			} else {
				require.NoDirExists(t, path, "unrelated dynamic mounts must not block eviction")
			}
		})
	}
}

func TestArtifactEvictionFilesystemAndReplay(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "model")
	require.NoError(t, os.Mkdir(path, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("weights"), 0600))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "customer", UID: "uid", Annotations: map[string]string{"ome.io/artifact-residency": "Evicted"}}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model"), Path: &path}}}
	key := constants.GetModelConfigMapKey(model.Namespace, model.Name, false)
	raw, err := json.Marshal(ModelEntry{Name: model.Name, ModelUID: model.UID, Status: ModelStatusReady})
	require.NoError(t, err)
	g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{key: string(raw)}), map[string]string{constants.GetBaseModelLabel(model.Namespace, model.Name): "Ready"})
	g.modelRootDir = root
	g.modelClient = omefake.NewSimpleClientset(model)
	g.kubeClient = g.configMapReconciler.kubeClient
	g.artifactRouting.known = true
	initializeArtifactEvictionTestNode(t, g)
	task := &GopherTask{TaskType: Evict, BaseModel: model}
	g.enqueueTask(task)
	queued, ok := g.taskQueue.popHighPriority()
	require.True(t, ok)
	require.True(t, queued.ResidencyManaged)
	require.NoError(t, g.processTask(queued))
	require.NoDirExists(t, path)
	fakeClient := g.kubeClient.(*k8sfake.Clientset)
	fakeClient.ClearActions()
	waiting, err := g.processArtifactEviction(context.Background(), task)
	require.NoError(t, err)
	require.False(t, waiting)
	for _, action := range fakeClient.Actions() {
		require.NotContains(t, []string{"update", "patch"}, action.GetVerb(), "stable Evicted replay must not churn node status")
	}
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: model.Namespace}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: model.Name}}}
	_, err = g.modelClient.OmeV1beta1().InferenceServices(model.Namespace).Create(context.Background(), service, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = g.processArtifactEviction(context.Background(), task)
	require.NoError(t, err)
	latest, err := g.modelClient.OmeV1beta1().BaseModels(model.Namespace).Get(context.Background(), model.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "Evicted", latest.Annotations["ome.io/artifact-residency"], "only the endpoint controller may release intent after recording its deadline")
}

func TestArtifactEvictionScoutIntent(t *testing.T) {
	for _, eligible := range []bool{true, false} {
		t.Run(map[bool]string{true: "eligible", false: "ineligible"}[eligible], func(t *testing.T) {
			queue := make(chan *GopherTask, 4)
			nodeType := "h100"
			if !eligible {
				nodeType = "b200"
			}
			w := &Scout{gopherChan: queue, nodeInfo: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"gpu": nodeType}}}, logger: zap.NewNop().Sugar()}
			old := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "customer"}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model"), NodeSelector: map[string]string{"gpu": "h100"}}}}
			evicted := old.DeepCopy()
			evicted.Annotations = map[string]string{"ome.io/artifact-residency": "Evicted"}
			w.updateBaseModel(old, evicted)
			if !eligible {
				require.Empty(t, queue)
				return
			}
			require.Len(t, queue, 1)
			require.Equal(t, GopherTaskType("Evict"), (<-queue).TaskType)
			changed := evicted.DeepCopy()
			changed.Labels = map[string]string{"unrelated": "label"}
			w.updateBaseModel(evicted, changed)
			require.Len(t, queue, 2)
			require.Equal(t, DownloadOverride, (<-queue).TaskType)
			require.Equal(t, Evict, (<-queue).TaskType)
			w.updateBaseModel(changed, old)
			require.Len(t, queue, 1)
			require.Equal(t, Download, (<-queue).TaskType)
		})
	}
}

func TestArtifactEvictionRestartReplayAndStaleDownloads(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"gpu": "h100"}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/nodes/node1", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(node))
	}))
	defer server.Close()
	kubeClient, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	queue := make(chan *GopherTask, 4)
	scout := &Scout{ctx: context.Background(), nodeName: node.Name, nodeInfo: node, kubeClient: kubeClient, gopherChan: queue, logger: zap.NewNop().Sugar()}
	base := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "customer", UID: "uid", Annotations: map[string]string{constants.ModelArtifactResidencyAnnotation: constants.ModelArtifactResidencyEvicted}}, Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{NodeSelector: map[string]string{"gpu": "h100"}}}}
	cluster := &v1beta1.ClusterBaseModel{ObjectMeta: *base.ObjectMeta.DeepCopy(), Spec: base.Spec}
	cluster.Namespace = ""
	scout.downloadBaseModel(base)
	scout.downloadClusterBaseModel(cluster)
	require.Len(t, queue, 4)
	for range 2 {
		require.Equal(t, Download, (<-queue).TaskType)
		require.Equal(t, Evict, (<-queue).TaskType)
	}
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, index.Add(base))
	clusterIndex := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, clusterIndex.Add(cluster))
	g := newGopherForProcessTask(makeConfigMap("node1", map[string]string{
		getModelID(base, nil):    modelEntryJSON(ModelStatusEvicted),
		getModelID(nil, cluster): modelEntryJSON(ModelStatusEvicted),
	}), nil)
	g.baseModelLister = modelslister.NewBaseModelLister(index)
	g.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(clusterIndex)
	oldBase, oldCluster := base.DeepCopy(), cluster.DeepCopy()
	oldBase.Annotations, oldCluster.Annotations = nil, nil
	for _, task := range []*GopherTask{{TaskType: Download, BaseModel: oldBase}, {TaskType: DownloadOverride, ClusterBaseModel: oldCluster}} {
		skip, deleteCR, err := g.shouldSkipStaleDownloadTask(context.Background(), task)
		require.NoError(t, err)
		require.True(t, skip)
		require.False(t, deleteCR)
	}
}
