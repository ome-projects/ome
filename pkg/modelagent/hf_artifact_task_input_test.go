package modelagent

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const ociInputTestSHA = "0123456789abcdef0123456789abcdef01234567"

func newOCIInputTestTask() *GopherTask {
	policy := v1beta1.ReuseIfExists
	uri := "oci://n/namespace/b/bucket/o/artifacts/Qwen/Qwen3-8B/" + ociInputTestSHA
	childPath := "/models/group/model-1"
	return &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Name: "model-1", Namespace: "default", UID: "model-uid",
				Annotations: map[string]string{hfModelIDAnnotationKey: "Qwen/Qwen3-8B", hfSHAAnnotationKey: ociInputTestSHA},
			},
			Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
				StorageUri: &uri, Path: &childPath, DownloadPolicy: &policy,
			}},
		},
	}
}

func TestNewHfArtifactTaskInputForOCIGates(t *testing.T) {
	tests := []struct {
		name   string
		change func(*GopherTask, **v1beta1.StorageSpec)
		want   bool
	}{
		{name: "download", want: true},
		{name: "override", change: func(task *GopherTask, _ **v1beta1.StorageSpec) { task.TaskType = DownloadOverride }, want: true},
		{name: "delete", change: func(task *GopherTask, _ **v1beta1.StorageSpec) { task.TaskType = Delete }},
		{name: "unknown task", change: func(task *GopherTask, _ **v1beta1.StorageSpec) { task.TaskType = "unknown" }},
		{name: "no storage", change: func(_ *GopherTask, spec **v1beta1.StorageSpec) { *spec = nil }},
		{name: "no URI", change: func(_ *GopherTask, spec **v1beta1.StorageSpec) { (*spec).StorageUri = nil }},
		{name: "no policy", change: func(_ *GopherTask, spec **v1beta1.StorageSpec) { (*spec).DownloadPolicy = nil }},
		{name: "empty child path", change: func(_ *GopherTask, spec **v1beta1.StorageSpec) { *(*spec).Path = "" }},
		{name: "always download", change: func(_ *GopherTask, spec **v1beta1.StorageSpec) { *(*spec).DownloadPolicy = v1beta1.AlwaysDownload }},
		{name: "direct HF", change: func(_ *GopherTask, spec **v1beta1.StorageSpec) { *(*spec).StorageUri = "hf://Qwen/Qwen3-8B" }},
		{name: "invalid URI", change: func(_ *GopherTask, spec **v1beta1.StorageSpec) {
			*(*spec).StorageUri = "oci://n/ns/wrong/bucket/o/Qwen/Qwen3-8B/" + ociInputTestSHA
		}},
		{name: "no annotations", change: func(task *GopherTask, _ **v1beta1.StorageSpec) { task.BaseModel.Annotations = nil }},
		{name: "missing model ID", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			delete(task.BaseModel.Annotations, hfModelIDAnnotationKey)
		}},
		{name: "missing SHA", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			delete(task.BaseModel.Annotations, hfSHAAnnotationKey)
		}},
		{name: "branch annotation", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			task.BaseModel.Annotations[hfSHAAnnotationKey] = "main"
		}},
		{name: "model traversal", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			task.BaseModel.Annotations[hfModelIDAnnotationKey] = "../model"
		}},
		{name: "shape partial copy", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: string(constants.ServingBaseModel), ShapeAlias: "H100"}
		}},
		{name: "shape partial copy without alias", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: string(constants.ServingBaseModel)}
		}},
		{name: "inactive shape filter", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{ModelType: string(constants.ServingBaseModel)}
		}, want: true},
		{name: "unfiltered TensorRT type", change: func(task *GopherTask, _ **v1beta1.StorageSpec) {
			task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: "other"}
		}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := newOCIInputTestTask()
			spec := task.BaseModel.Spec.Storage
			if tt.change != nil {
				tt.change(task, &spec)
			}
			input, ok, err := newHfArtifactTaskInputForOCI(task, spec, "/models")
			require.NoError(t, err)
			require.Equal(t, tt.want, ok)
			if !ok {
				require.Equal(t, hfArtifactTaskInput{}, input)
			}
		})
	}
	for _, task := range []*GopherTask{nil, {}, {TaskType: Download}, {TaskType: Download, BaseModel: &v1beta1.BaseModel{}, ClusterBaseModel: &v1beta1.ClusterBaseModel{}}} {
		_, ok, err := newHfArtifactTaskInputForOCI(task, newOCIInputTestTask().BaseModel.Spec.Storage, "/models")
		require.NoError(t, err)
		require.False(t, ok)
	}
}

func TestNewHfArtifactTaskInputForOCIPrefix(t *testing.T) {
	tests := []struct {
		name, prefix string
		want         bool
	}{
		{"exact", "Qwen/Qwen3-8B/" + ociInputTestSHA, true},
		{"nested", "group/artifacts/Qwen/Qwen3-8B/" + ociInputTestSHA, true},
		{"trailing slash", "Qwen/Qwen3-8B/" + ociInputTestSHA + "/", true},
		{"uppercase SHA", "Qwen/Qwen3-8B/" + strings.ToUpper(ociInputTestSHA), true},
		{"model case mismatch", "qwen/Qwen3-8B/" + ociInputTestSHA, false},
		{"model suffix not segment", "NotQwen/Qwen3-8B/" + ociInputTestSHA, false},
		{"different SHA", "Qwen/Qwen3-8B/" + strings.Repeat("a", 40), false},
		{"branch", "Qwen/Qwen3-8B/main", false},
		{"nested under commit", "Qwen/Qwen3-8B/" + ociInputTestSHA + "/weights", false},
		{"traversal inside identity", "Qwen/Qwen3-8B/ignored/../" + ociInputTestSHA, false},
		{"traversal before identity", "artifacts/../Qwen/Qwen3-8B/" + ociInputTestSHA, false},
		{"dot segment", "artifacts/./Qwen/Qwen3-8B/" + ociInputTestSHA, false},
		{"duplicate slash", "artifacts//Qwen/Qwen3-8B/" + ociInputTestSHA, false},
		{"duplicate trailing slash", "Qwen/Qwen3-8B/" + ociInputTestSHA + "//", false},
		{"encoded model separator", "Qwen%2FQwen3-8B/" + ociInputTestSHA, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := newOCIInputTestTask()
			task.BaseModel.Annotations[hfSHAAnnotationKey] = " " + strings.ToUpper(ociInputTestSHA) + " "
			spec := task.BaseModel.Spec.Storage
			*spec.StorageUri = "oci://n/ns/b/bucket/o/" + tt.prefix
			input, ok, err := newHfArtifactTaskInputForOCI(task, spec, "/models")
			require.NoError(t, err)
			require.Equal(t, tt.want, ok)
			if ok {
				require.Equal(t, ociInputTestSHA, input.Parent.Identity.CommitSHA)
			}
		})
	}
}

func TestNewHfArtifactTaskInputForOCIPaths(t *testing.T) {
	tests := []struct {
		name, root, child, parentRoot, scanRoot string
		cluster                                 bool
	}{
		{"base", "/models", "/models/group/model-1", "/models/group", "/models", false},
		{"base without root", "", "/models/group/model-1", "/models/group", "/models/group", false},
		{"base custom storage root", "/models", "/custom/model-1", "/custom", "/custom", false},
		{"base prefix sibling", "/models", "/models-other/model-1", "/models-other", "/models-other", false},
		{"cluster", "/models", "/models/openai/model-1", "/models", "/models", true},
		{"cluster without root", "", "/models/openai/model-1", "/models/openai", "/models/openai", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := newOCIInputTestTask()
			spec := task.BaseModel.Spec.Storage
			*spec.Path = tt.child
			wantKey := "default.basemodel.model-1"
			if tt.cluster {
				task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
				task.ClusterBaseModel.Namespace = ""
				task.BaseModel = nil
				wantKey = "clusterbasemodel.model-1"
			}
			input, ok, err := newHfArtifactTaskInputForOCI(task, spec, tt.root)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, wantKey, input.ChildModelKey)
			require.EqualValues(t, "model-uid", input.ChildModelUID)
			require.Equal(t, tt.child, input.ChildModelPath)
			require.Equal(t, tt.scanRoot, input.ModelStoreRoot)
			require.Equal(t, filepath.Join(tt.parentRoot, constants.ModelArtifactsDirectory, "Qwen", "Qwen3-8B", ociInputTestSHA), input.Parent.LocalPath)
			require.Equal(t, hfArtifactConfigMapKey(input.Parent.Identity), input.Parent.Key)
			require.Empty(t, input.Parent.LockID)
			require.Empty(t, input.Parent.Children)
		})
	}
}

func TestNewHfArtifactTaskInputForOCIRejectsUnsafePaths(t *testing.T) {
	tests := []struct {
		name, root, child string
		cluster           bool
	}{
		{"relative child", "/models", "model-1", false},
		{"child traversal", "/models", "/models/../outside/model-1", false},
		{"root traversal", "/models/../models", "/models/model-1", false},
		{"relative root", "models", "/models/model-1", false},
		{"root child", "", "/", false},
		{"child equals scan root", "/models", "/models", false},
		{"child is root ancestor", "/models/nested", "/models", false},
		{"child in parent namespace", "/models", "/models/_artifacts/Qwen/model-1", false},
		{"child equals parent", "/models", "/models/_artifacts/Qwen/Qwen3-8B/" + ociInputTestSHA, true},
		{"cluster outside root", "/models", "/custom/model-1", true},
		{"cluster prefix sibling", "/models", "/models-other/model-1", true},
		{"child whitespace", "/models", " /models/model-1 ", false},
		{"child NUL", "/models", "/models/model\x00", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := newOCIInputTestTask()
			spec := task.BaseModel.Spec.Storage
			*spec.Path = tt.child
			if tt.cluster {
				task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
				task.BaseModel = nil
			}
			input, ok, err := newHfArtifactTaskInputForOCI(task, spec, tt.root)
			require.Error(t, err)
			require.False(t, ok)
			require.Equal(t, hfArtifactTaskInput{}, input)
		})
	}
	task := newOCIInputTestTask()
	task.BaseModel.Spec.Storage.Path = nil
	_, ok, err := newHfArtifactTaskInputForOCI(task, task.BaseModel.Spec.Storage, "/models")
	require.Error(t, err)
	require.False(t, ok)
}
