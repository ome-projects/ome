package modelagent

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDirectHfRoutingMatchesReuseEligibility(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		kind := "BaseModel"
		if cluster {
			kind = "ClusterBaseModel"
		}
		for _, taskType := range []GopherTaskType{Download, DownloadOverride, Delete} {
			for _, tc := range []struct {
				name   string
				change func(*GopherTask, *v1beta1.StorageSpec)
				shared bool
			}{
				{name: "eligible", shared: true},
				{name: "missing path", change: func(_ *GopherTask, spec *v1beta1.StorageSpec) { spec.Path = nil }},
				{name: "empty path", change: func(_ *GopherTask, spec *v1beta1.StorageSpec) { spec.Path = stringPtr("") }},
				{name: "missing policy", change: func(_ *GopherTask, spec *v1beta1.StorageSpec) { spec.DownloadPolicy = nil }},
				{name: "always download", change: func(_ *GopherTask, spec *v1beta1.StorageSpec) { *spec.DownloadPolicy = v1beta1.AlwaysDownload }},
				{name: "TensorRT serving base model", change: func(task *GopherTask, _ *v1beta1.StorageSpec) {
					task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: string(constants.ServingBaseModel)}
				}},
				{name: "inactive shape filter", shared: true, change: func(task *GopherTask, _ *v1beta1.StorageSpec) {
					task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{ModelType: string(constants.ServingBaseModel)}
				}},
				{name: "other TensorRT model type", shared: true, change: func(task *GopherTask, _ *v1beta1.StorageSpec) {
					task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: "other"}
				}},
			} {
				t.Run(kind+"/"+string(taskType)+"/"+tc.name, func(t *testing.T) {
					s, task, _ := newTestHfArtifactGopher(t)
					task.TaskType = taskType
					spec := task.BaseModel.Spec.Storage
					spec.StorageUri = stringPtr("hf://Org/Model@main")
					if tc.change != nil {
						tc.change(task, spec)
					}
					if cluster {
						task.ClusterBaseModel = &v1beta1.ClusterBaseModel{ObjectMeta: task.BaseModel.ObjectMeta, Spec: task.BaseModel.Spec}
						task.ClusterBaseModel.Namespace = ""
						task.BaseModel = nil
					}
					require.NoError(t, s.artifactRouting.observeSnapshot(nil))
					s.artifactRouting.mutex.Lock()
					s.routeArtifactTaskLocked(task)
					s.artifactRouting.mutex.Unlock()
					assert.Equal(t, tc.shared, task.SharedArtifact)
					assert.Equal(t, !tc.shared, s.isOrdinaryArtifactTask(task))
					assert.Equal(t, tc.shared, s.artifactRouting.children[getModelID(task.BaseModel, task.ClusterBaseModel)])
				})
			}
		}
	}
}

func TestDirectHfRoutingPreservesExistingSharedOwnership(t *testing.T) {
	for _, evidence := range []string{"recorded child", "shared symlink", "previously shared task"} {
		t.Run(evidence, func(t *testing.T) {
			s, task, input := newTestHfArtifactGopher(t)
			task.TaskType = Delete
			task.BaseModel.Spec.Storage.StorageUri = stringPtr("hf://Org/Model@main")
			task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: string(constants.ServingBaseModel)}
			require.NoError(t, s.artifactRouting.observeSnapshot(nil))
			switch evidence {
			case "recorded child":
				s.artifactRouting.children[input.ChildModelKey] = true
				task.BaseModel.Spec.Storage.Path = nil
			case "shared symlink":
				require.NoError(t, os.Symlink(input.Parent.LocalPath, input.ChildModelPath))
			case "previously shared task":
				task.SharedArtifact = true
				task.BaseModel.Spec.Storage.Path = stringPtr("")
			}
			s.artifactRouting.mutex.Lock()
			s.routeArtifactTaskLocked(task)
			s.artifactRouting.mutex.Unlock()
			assert.True(t, task.SharedArtifact)
			assert.False(t, s.isOrdinaryArtifactTask(task))
			assert.True(t, s.artifactRouting.children[input.ChildModelKey])
		})
	}
}
