package modelagent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	modelslister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestArtifactChildReplayPreservesShapeFilter(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, tc := range []struct {
			name       string
			format     string
			modelType  string
			shapeLabel string
			shape      string
			wantError  bool
		}{
			{name: "format changed to TensorRT", format: constants.TensorRTLLM, shapeLabel: constants.NodeInstanceShapeLabel, shape: "BM.GPU.H100.8"},
			{name: "deprecated shape label", format: constants.TensorRTLLM, shapeLabel: constants.DeprecatedNodeInstanceShapeLabel, shape: "BM.GPU.H100.8"},
			{name: "node missing", format: constants.TensorRTLLM, wantError: true},
			{name: "unknown shape retains alias", format: constants.TensorRTLLM, shapeLabel: constants.NodeInstanceShapeLabel, shape: "unknown"},
			{name: "non serving TensorRT", format: constants.TensorRTLLM, modelType: "other"},
			{name: "ordinary shared model", format: "safetensors"},
		} {
			t.Run(tc.name+map[bool]string{false: "/BaseModel", true: "/ClusterBaseModel"}[cluster], func(t *testing.T) {
				ctx := context.Background()
				g, task, input := newTestHfArtifactGopher(t)
				defer g.taskQueue.close()
				require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
				model := task.BaseModel.DeepCopy()
				model.Spec.ModelFormat.Name = tc.format
				model.Spec.AdditionalMetadata = map[string]string{}
				if tc.modelType != "" {
					model.Spec.AdditionalMetadata["type"] = tc.modelType
				}
				model.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-1"
				index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
				key := getModelID(model, nil)
				if cluster {
					cbm := &v1beta1.ClusterBaseModel{ObjectMeta: model.ObjectMeta, Spec: model.Spec}
					cbm.Namespace = ""
					require.NoError(t, index.Add(cbm))
					g.clusterBaseModelLister = modelslister.NewClusterBaseModelLister(index)
					key = getModelID(nil, cbm)
				} else {
					require.NoError(t, index.Add(model))
					g.baseModelLister = modelslister.NewBaseModelLister(index)
				}
				g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, g.kubeClient, 1, g.logger)
				if tc.shapeLabel != "" {
					_, err := g.kubeClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
						Name: g.configMapReconciler.nodeName, Labels: map[string]string{tc.shapeLabel: tc.shape},
					}}, metav1.CreateOptions{})
					require.NoError(t, err)
				}
				err := g.updateHfArtifactChildLabels(ctx, map[string]ModelStatus{key: ModelStatusReady})
				if tc.wantError {
					require.Error(t, err)
					require.Zero(t, g.taskQueue.len(), "do not enqueue an unfiltered fallback")
					return
				}
				require.NoError(t, err)
				replay, ok := g.taskQueue.popHighPriority()
				require.True(t, ok)
				require.True(t, replay.ArtifactRequestReplay)
				require.NotNil(t, replay.TensorRTLLMShapeFilter)
				filter := replay.TensorRTLLMShapeFilter
				require.Equal(t, tc.format == constants.TensorRTLLM, filter.IsTensorrtLLMModel)
				modelType := tc.modelType
				if modelType == "" {
					modelType = string(constants.ServingBaseModel)
				}
				require.Equal(t, modelType, filter.ModelType)
				filtered := tc.format == constants.TensorRTLLM && modelType == string(constants.ServingBaseModel)
				if filtered {
					alias := "H100"
					if tc.shape == "unknown" {
						alias = tc.shape
					}
					require.Equal(t, alias, filter.ShapeAlias)
				}
				_, eligible, err := newHfArtifactTaskInputForOCI(replay, taskModelSpec(replay).Storage, g.modelRootDir)
				require.NoError(t, err)
				require.Equal(t, !filtered, eligible)
			})
		}
	}
}
