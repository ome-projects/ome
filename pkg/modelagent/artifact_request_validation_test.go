package modelagent

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func newArtifactRequestValidationTest(t *testing.T) (*Gopher, *GopherTask) {
	t.Helper()
	repository, kube := newTestHfArtifactRepository(t, nil)
	maps := repository.configMaps
	_, err := kube.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: maps.nodeName, UID: "node-uid", Labels: map[string]string{"pool": "gpu"},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	model := createTestBaseModel()
	model.UID = "model-uid"
	model.Spec.Storage = &v1beta1.StorageSpec{StorageUri: stringPtr("hf://org/model"), Path: stringPtr("/models/model")}
	return &Gopher{modelClient: omefake.NewSimpleClientset(model), kubeClient: kube,
		configMapReconciler: maps, logger: maps.logger, nodeUID: "node-uid"}, &GopherTask{TaskType: Download, BaseModel: model}
}

func TestArtifactRequestChecksPlacementAndIdentityWithOneNodeRead(t *testing.T) {
	g, task := newArtifactRequestValidationTest(t)
	live := task.BaseModel.DeepCopy()
	live.Spec.Storage.NodeSelector = map[string]string{"pool": "gpu"}
	g.modelClient = omefake.NewSimpleClientset(live)
	kube := g.kubeClient.(*kubefake.Clientset)
	kube.ClearActions()

	require.NoError(t, g.validateArtifactDownload(context.Background(), task))
	nodeReads := 0
	for _, action := range kube.Actions() {
		if action.Matches("get", "nodes") {
			nodeReads++
		}
	}
	require.Equal(t, 1, nodeReads)
}

func TestArtifactRequestOnlyReadsNodeWhenNeeded(t *testing.T) {
	for _, tc := range []struct {
		name      string
		unpinned  bool
		placement bool
		stale     bool
		reads     int
	}{
		{name: "pinned without placement", reads: 1},
		{name: "unpinned without placement", unpinned: true},
		{name: "unpinned with placement", unpinned: true, placement: true, reads: 1},
		{name: "stale model", placement: true, stale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newArtifactRequestValidationTest(t)
			if tc.unpinned {
				g.nodeUID = ""
			}
			live := task.BaseModel.DeepCopy()
			if tc.placement {
				live.Spec.Storage.NodeSelector = map[string]string{"pool": "gpu"}
			}
			if tc.stale {
				live.UID = "replacement"
			}
			g.modelClient = omefake.NewSimpleClientset(live)
			nodeReads := 0
			g.kubeClient.(*kubefake.Clientset).PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				nodeReads++
				return false, nil, nil
			})
			err := g.validateArtifactDownload(context.Background(), task)
			require.Equal(t, tc.stale, err != nil)
			require.Equal(t, tc.reads, nodeReads)
		})
	}
}

func TestArtifactRequestReadsFreshNodeAfterChanges(t *testing.T) {
	for _, change := range []string{"labels", "UID"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			g, task := newArtifactRequestValidationTest(t)
			live := task.BaseModel.DeepCopy()
			live.Spec.Storage.NodeSelector = map[string]string{"pool": "gpu"}
			g.modelClient = omefake.NewSimpleClientset(live)
			require.NoError(t, g.validateArtifactDownload(ctx, task))
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			if change == "labels" {
				node.Labels["pool"] = "other"
			} else {
				node.UID = "replacement"
			}
			_, err = g.kubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.Error(t, g.validateArtifactDownload(ctx, task))
		})
	}
}

func TestArtifactEvictionAdmissionSkipsNodeButValidationChecksIdentity(t *testing.T) {
	for _, identity := range []string{"pinned", "unpinned", "replaced"} {
		t.Run(identity, func(t *testing.T) {
			g, task := newArtifactRequestValidationTest(t)
			task.TaskType = Evict
			task.BaseModel.Annotations = map[string]string{ArtifactResidencyAnnotation: string(ModelStatusEvicted)}
			live := task.BaseModel.DeepCopy()
			live.Spec.Storage.NodeSelector = map[string]string{"pool": "other"}
			g.modelClient = omefake.NewSimpleClientset(live)
			if identity == "unpinned" {
				g.nodeUID = ""
			} else if identity == "replaced" {
				g.nodeUID = "old-node"
			}
			nodeReads := 0
			g.kubeClient.(*kubefake.Clientset).PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				nodeReads++
				return false, nil, nil
			})
			current, err := g.currentArtifactTask(context.Background(), task)
			require.NoError(t, err)
			require.NotNil(t, current)
			require.Equal(t, live.Spec.Storage, taskModelSpec(current).Storage)
			require.Zero(t, nodeReads)
			err = g.validateArtifactDownload(context.Background(), task)
			if identity == "replaced" {
				require.ErrorContains(t, err, "node identity changed")
			} else {
				require.NoError(t, err)
			}
			wantNodeReads := 1
			if identity == "unpinned" {
				wantNodeReads = 0
			}
			require.Equal(t, wantNodeReads, nodeReads)
		})
	}
}

func TestArtifactRequestPropagatesLookupErrors(t *testing.T) {
	for _, resource := range []string{"basemodels", "nodes"} {
		t.Run(resource, func(t *testing.T) {
			g, task := newArtifactRequestValidationTest(t)
			apiErr := errors.New("lookup unavailable")
			fail := func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, apiErr }
			if resource == "basemodels" {
				g.modelClient.(*omefake.Clientset).PrependReactor("get", resource, fail)
			} else {
				g.kubeClient.(*kubefake.Clientset).PrependReactor("get", resource, fail)
			}
			require.ErrorIs(t, g.validateArtifactDownload(context.Background(), task), apiErr)
		})
	}
}

func TestArtifactRequestUsesLiveIdentityAndInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.BaseModel)
		skip   bool
	}{
		{"same", func(*v1beta1.BaseModel) {}, false},
		{"replacement", func(m *v1beta1.BaseModel) { m.UID = "replacement" }, true},
		{"deleting", func(m *v1beta1.BaseModel) { now := metav1.Now(); m.DeletionTimestamp = &now }, true},
		{"new request", func(m *v1beta1.BaseModel) {
			m.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "R2"}
		}, true},
		{"new path", func(m *v1beta1.BaseModel) { m.Spec.Storage.Path = stringPtr("/models/other") }, true},
		{"new source", func(m *v1beta1.BaseModel) { m.Spec.Storage.StorageUri = stringPtr("hf://org/other") }, true},
		{"default policy", func(m *v1beta1.BaseModel) { p := v1beta1.AlwaysDownload; m.Spec.Storage.DownloadPolicy = &p }, false},
		{"changed HF policy", func(m *v1beta1.BaseModel) { p := v1beta1.ReuseIfExists; m.Spec.Storage.DownloadPolicy = &p }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newArtifactRequestValidationTest(t)
			live := task.BaseModel.DeepCopy()
			tc.change(live)
			g.modelClient = omefake.NewSimpleClientset(live)
			current, err := g.currentArtifactTask(context.Background(), task)
			require.NoError(t, err)
			require.Equal(t, tc.skip, current == nil)
		})
	}
}

func TestArtifactRequestRequiresCurrentNodeAndValidMarker(t *testing.T) {
	for _, boundary := range []string{"valid", "replaced node", "invalid marker"} {
		t.Run(boundary, func(t *testing.T) {
			g, task := newArtifactRequestValidationTest(t)
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "R2"}
			if boundary == "invalid marker" {
				task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "invalid marker"
			}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			if boundary == "replaced node" {
				g.nodeUID = "old-node"
			}
			err := g.validateArtifactDownload(context.Background(), task)
			if boundary == "valid" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestArtifactCleanupDeleteRequiresOriginalModelUID(t *testing.T) {
	for _, state := range []string{"absent", "same UID", "replacement UID", "API error"} {
		t.Run(state, func(t *testing.T) {
			g, task, input := newSharedEvictionTestModel(t)
			task.TaskType = Delete
			apiErr := errors.New("model lookup unavailable")
			switch state {
			case "absent":
				g.modelClient = omefake.NewSimpleClientset()
			case "replacement UID":
				latest := task.BaseModel.DeepCopy()
				latest.UID = "replacement"
				g.modelClient = omefake.NewSimpleClientset(latest)
			case "API error":
				g.modelClient.(*omefake.Clientset).PrependReactor("get", "basemodels", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, apiErr
				})
			}
			for _, validate := range []func() error{
				func() error { return g.validateArtifactCleanupRequest(context.Background(), task) },
				func() error {
					_, err := g.validateSharedCleanupOwnership(context.Background(), task, input)
					return err
				},
			} {
				err := validate()
				switch state {
				case "absent", "same UID":
					require.NoError(t, err)
				case "replacement UID":
					require.Error(t, err)
				case "API error":
					require.ErrorIs(t, err, apiErr)
				}
			}
		})
	}
}
