package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestArtifactRequestUsesLiveIdentityAndInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.BaseModel)
		skip   bool
	}{
		{"same", func(*v1beta1.BaseModel) {}, false},
		{"replacement", func(m *v1beta1.BaseModel) { m.UID = "replacement" }, true},
		{"new request", func(m *v1beta1.BaseModel) {
			m.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "R2"}
		}, true},
		{"new path", func(m *v1beta1.BaseModel) { m.Spec.Storage.Path = stringPtr("/models/other") }, true},
		{"new source", func(m *v1beta1.BaseModel) { m.Spec.Storage.StorageUri = stringPtr("hf://org/other") }, true},
		{"eligible placement", func(m *v1beta1.BaseModel) { m.Spec.Storage.NodeSelector = map[string]string{"pool": "gpu"} }, false},
		{"ineligible placement", func(m *v1beta1.BaseModel) { m.Spec.Storage.NodeSelector = map[string]string{"pool": "other"} }, true},
		{"default policy", func(m *v1beta1.BaseModel) { p := v1beta1.AlwaysDownload; m.Spec.Storage.DownloadPolicy = &p }, false},
		{"changed HF policy", func(m *v1beta1.BaseModel) { p := v1beta1.ReuseIfExists; m.Spec.Storage.DownloadPolicy = &p }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newArtifactRequestValidationTest(t)
			live := task.BaseModel.DeepCopy()
			tc.change(live)
			g.modelClient = omefake.NewSimpleClientset(live)
			skip, _, err := g.shouldSkipArtifactTask(context.Background(), task)
			require.NoError(t, err)
			require.Equal(t, tc.skip, skip)
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
