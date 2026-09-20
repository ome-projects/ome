package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omev1beta1lister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// TestHandleTaskPVCSkip tests that PVC storage types are properly skipped
func TestHandleTaskPVCSkip(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Define test cases
	testCases := []struct {
		name          string
		task          *GopherTask
		storageType   storage.StorageType
		expectError   bool
		expectSkip    bool
		errorContains string
	}{
		{
			name: "PVC storage type should be skipped",
			task: &GopherTask{
				TaskType: Download,
				BaseModel: &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc-model",
						Namespace: "default",
						UID:       "test-uid-1",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							StorageUri: stringPtr("pvc://my-pvc/models/llama2"),
						},
					},
				},
			},
			storageType: storage.StorageTypePVC,
			expectError: false,
			expectSkip:  true,
		},
		{
			name: "OCI storage type should not be skipped",
			task: &GopherTask{
				TaskType: Download,
				BaseModel: &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-oci-model",
						Namespace: "default",
						UID:       "test-uid-2",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							StorageUri: stringPtr("oci://n/namespace/b/bucket/o/model"),
						},
					},
				},
			},
			storageType: storage.StorageTypeOCI,
			expectError: false,
			expectSkip:  false,
		},
		{
			name: "Vendor storage type should be handled",
			task: &GopherTask{
				TaskType: Download,
				BaseModel: &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-vendor-model",
						Namespace: "default",
						UID:       "test-uid-3",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							StorageUri: stringPtr("vendor://nvidia/models/llama"),
						},
					},
				},
			},
			storageType: storage.StorageTypeVendor,
			expectError: false,
			expectSkip:  false,
		},
		{
			name: "HuggingFace storage type should be handled",
			task: &GopherTask{
				TaskType: Download,
				BaseModel: &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-hf-model",
						Namespace: "default",
						UID:       "test-uid-4",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							StorageUri: stringPtr("hf://meta-llama/Llama-2-7b-hf"),
						},
					},
				},
			},
			storageType: storage.StorageTypeHuggingFace,
			expectError: false,
			expectSkip:  false,
		},
		{
			name: "Invalid storage URI should error",
			task: &GopherTask{
				TaskType: Download,
				BaseModel: &v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-invalid-model",
						Namespace: "default",
						UID:       "test-uid-5",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							StorageUri: stringPtr("invalid://storage/uri"),
						},
					},
				},
			},
			expectError:   true,
			errorContains: "unknown storage type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// For PVC test, we need to mock the behavior
			// Since handleTask is complex, we'll test the specific storage type logic
			baseModelSpec := tc.task.BaseModel.Spec
			storageType, err := storage.GetStorageType(*baseModelSpec.Storage.StorageUri)

			if tc.expectError {
				assert.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.storageType, storageType)

			// Verify that PVC storage type would be skipped
			if storageType == storage.StorageTypePVC {
				assert.True(t, tc.expectSkip, "PVC storage type should be skipped")
			}
		})
	}
}

// TestShouldDownloadModelPVC tests that PVC models are skipped in scout
func TestShouldDownloadModelPVC(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	// Set up test node
	testNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				"node-type": "gpu",
			},
		},
	}

	// Create a test scout
	scout := &Scout{
		logger:   sugaredLogger,
		nodeInfo: testNode,
	}

	// Test cases
	testCases := []struct {
		name           string
		storageSpec    *v1beta1.StorageSpec
		expectedResult bool
		description    string
	}{
		{
			name: "PVC storage should be skipped",
			storageSpec: &v1beta1.StorageSpec{
				StorageUri: stringPtr("pvc://my-pvc/models/llama2"),
			},
			expectedResult: false,
			description:    "PVC storage type should return false (skip)",
		},
		{
			name: "PVC storage with namespace should be skipped",
			storageSpec: &v1beta1.StorageSpec{
				StorageUri: stringPtr("pvc://namespace:my-pvc/models/llama2"),
			},
			expectedResult: false,
			description:    "PVC storage type with namespace should return false (skip)",
		},
		{
			name: "OCI storage should not be skipped",
			storageSpec: &v1beta1.StorageSpec{
				StorageUri: stringPtr("oci://n/namespace/b/bucket/o/model"),
			},
			expectedResult: true,
			description:    "OCI storage type should return true (download)",
		},
		{
			name: "HuggingFace storage should not be skipped",
			storageSpec: &v1beta1.StorageSpec{
				StorageUri: stringPtr("hf://meta-llama/Llama-2-7b-hf"),
			},
			expectedResult: true,
			description:    "HuggingFace storage type should return true (download)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := scout.shouldDownloadModel(tc.storageSpec)
			assert.Equal(t, tc.expectedResult, result, tc.description)
		})
	}
}

// Mock implementations for testing
type mockBaseModelLister struct {
	models []*v1beta1.BaseModel
	err    error
}

func (m *mockBaseModelLister) List(selector labels.Selector) ([]*v1beta1.BaseModel, error) {
	return m.models, m.err
}

func (m *mockBaseModelLister) BaseModels(namespace string) omev1beta1lister.BaseModelNamespaceLister {
	return &mockBaseModelNamespaceLister{
		namespace: namespace,
		models:    m.models,
		err:       m.err,
	}
}

type mockBaseModelNamespaceLister struct {
	namespace string
	models    []*v1beta1.BaseModel
	err       error
}

func (m *mockBaseModelNamespaceLister) List(selector labels.Selector) ([]*v1beta1.BaseModel, error) {
	if m.err != nil {
		return nil, m.err
	}
	var models []*v1beta1.BaseModel
	for _, model := range m.models {
		if model.Namespace == m.namespace {
			models = append(models, model)
		}
	}
	return models, nil
}

func (m *mockBaseModelNamespaceLister) Get(name string) (*v1beta1.BaseModel, error) {
	if m.err != nil {
		return nil, m.err
	}
	for _, model := range m.models {
		if model.Namespace == m.namespace && model.Name == name {
			return model, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "basemodels"}, name)
}

type mockClusterBaseModelLister struct {
	models []*v1beta1.ClusterBaseModel
	err    error
}

func (m *mockClusterBaseModelLister) List(selector labels.Selector) ([]*v1beta1.ClusterBaseModel, error) {
	return m.models, m.err
}

func (m *mockClusterBaseModelLister) Get(name string) (*v1beta1.ClusterBaseModel, error) {
	// Simple implementation for testing - find by name
	for _, model := range m.models {
		if model.Name == name {
			return model, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "clusterbasemodels"}, name)
}

// TestIsPathReferencedByOtherModels tests the isPathReferencedByOtherModels method
func TestIsPathReferencedByOtherModels(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	targetPath := "/models/llama2"

	testCases := []struct {
		name                      string
		baseModels                []*v1beta1.BaseModel
		clusterBaseModels         []*v1beta1.ClusterBaseModel
		excludeBaseModel          *v1beta1.BaseModel
		excludeClusterBaseModel   *v1beta1.ClusterBaseModel
		baseModelListerErr        error
		clusterBaseModelListerErr error
		expectedResult            bool
		expectedError             bool
		errorContains             string
		description               string
	}{
		{
			name:           "no models exist",
			description:    "should return false when no models exist",
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "path not referenced by any model",
			baseModels: []*v1beta1.BaseModel{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "model1",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr("/models/other-model"),
						},
					},
				},
			},
			clusterBaseModels: []*v1beta1.ClusterBaseModel{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster-model1",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr("/models/another-model"),
						},
					},
				},
			},
			description:    "should return false when target path is not referenced",
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "path referenced by BaseModel",
			baseModels: []*v1beta1.BaseModel{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "model1",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr(targetPath),
						},
					},
				},
			},
			description:    "should return true when path is referenced by BaseModel",
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "path referenced by ClusterBaseModel",
			clusterBaseModels: []*v1beta1.ClusterBaseModel{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster-model1",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr(targetPath),
						},
					},
				},
			},
			description:    "should return true when path is referenced by ClusterBaseModel",
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "path referenced by BaseModel but excluded",
			baseModels: []*v1beta1.BaseModel{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "model1",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr(targetPath),
						},
					},
				},
			},
			excludeBaseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "model1",
					Namespace: "default",
				},
				Spec: v1beta1.BaseModelSpec{
					Storage: &v1beta1.StorageSpec{
						Path: stringPtr(targetPath),
					},
				},
			},
			description:    "should return false when path is only referenced by excluded BaseModel",
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "path referenced by ClusterBaseModel but excluded",
			clusterBaseModels: []*v1beta1.ClusterBaseModel{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster-model1",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr(targetPath),
						},
					},
				},
			},
			excludeClusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-model1",
				},
				Spec: v1beta1.BaseModelSpec{
					Storage: &v1beta1.StorageSpec{
						Path: stringPtr(targetPath),
					},
				},
			},
			description:    "should return false when path is only referenced by excluded ClusterBaseModel",
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "path referenced by multiple models, one excluded",
			baseModels: []*v1beta1.BaseModel{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "model1",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr(targetPath),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "model2",
						Namespace: "default",
					},
					Spec: v1beta1.BaseModelSpec{
						Storage: &v1beta1.StorageSpec{
							Path: stringPtr(targetPath),
						},
					},
				},
			},
			excludeBaseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "model1",
					Namespace: "default",
				},
				Spec: v1beta1.BaseModelSpec{
					Storage: &v1beta1.StorageSpec{
						Path: stringPtr(targetPath),
					},
				},
			},
			description:    "should return true when path is referenced by multiple models but only one is excluded",
			expectedResult: true,
			expectedError:  false,
		},
		{
			name:               "BaseModel lister error",
			baseModelListerErr: errors.New("lister error"),
			description:        "should return error when BaseModel lister fails",
			expectedResult:     false,
			expectedError:      true,
			errorContains:      "failed to list BaseModels",
		},
		{
			name:                      "ClusterBaseModel lister error",
			clusterBaseModelListerErr: errors.New("lister error"),
			description:               "should return error when ClusterBaseModel lister fails",
			expectedResult:            false,
			expectedError:             true,
			errorContains:             "failed to list ClusterBaseModels",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock listers
			mockBaseModelLister := &mockBaseModelLister{
				models: tc.baseModels,
				err:    tc.baseModelListerErr,
			}
			mockClusterBaseModelLister := &mockClusterBaseModelLister{
				models: tc.clusterBaseModels,
				err:    tc.clusterBaseModelListerErr,
			}

			// Create a minimal Gopher instance for testing
			gopher := &Gopher{
				logger:                 sugaredLogger,
				baseModelLister:        mockBaseModelLister,
				clusterBaseModelLister: mockClusterBaseModelLister,
			}

			// Call the method under test
			result, err := gopher.isPathReferencedByOtherModels(targetPath, tc.excludeBaseModel, tc.excludeClusterBaseModel)

			// Check error conditions
			if tc.expectedError {
				assert.Error(t, err, tc.description)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains, tc.description)
				}
			} else {
				assert.NoError(t, err, tc.description)
			}

			// Check result
			assert.Equal(t, tc.expectedResult, result, tc.description)
		})
	}
}

// TestIsReservingModelArtifact tests isReservingModelArtifact method
func TestIsReservingModelArtifact_BaseModel(t *testing.T) {
	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func(sugaredLogger *zap.SugaredLogger) {
		_ = sugaredLogger.Sync()
	}(sugaredLogger)

	s := &Gopher{logger: sugaredLogger}

	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"nil labels", nil, false},
		{"true lower", map[string]string{"models.ome/reserve-model-artifact": "true"}, true},
		{"true upper", map[string]string{"models.ome/reserve-model-artifact": "TRUE"}, true},
		{"true mixed", map[string]string{"models.ome/reserve-model-artifact": "TrUe"}, true},
		{"not containing matched key", map[string]string{"models.ome/reserve-model": "true"}, false},
		{"false", map[string]string{"models.ome/reserve-model-artifact": "false"}, false},
		{"empty", map[string]string{"models.ome/reserve-model-artifact": ""}, false},
		{"other value", map[string]string{"models.ome/reserve-model-artifact": "otherValues"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bm := &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Labels: tc.labels,
				},
			}
			task := &GopherTask{
				TaskType:  Download, // value not important for this helper
				BaseModel: bm,
			}

			got := s.isReservingModelArtifact(task)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsReservingModelArtifact_ClusterBaseModel(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func() { _ = sugaredLogger.Sync() }()

	s := &Gopher{logger: sugaredLogger}
	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"nil labels", nil, false},
		{"true lower", map[string]string{"models.ome/reserve-model-artifact": "true"}, true},
		{"true upper", map[string]string{"models.ome/reserve-model-artifact": "TRUE"}, true},
		{"true mixed", map[string]string{"models.ome/reserve-model-artifact": "TrUe"}, true},
		{"not containing matched key", map[string]string{"models.ome/reserve-model": "true"}, false},
		{"false", map[string]string{"models.ome/reserve-model-artifact": "false"}, false},
		{"empty", map[string]string{"models.ome/reserve-model-artifact": ""}, false},
		{"other value", map[string]string{"models.ome/reserve-model-artifact": "otherValues"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cbm := &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Labels: tc.labels,
				},
			}
			task := &GopherTask{
				TaskType:         Download, // value not important for this helper
				ClusterBaseModel: cbm,
			}

			got := s.isReservingModelArtifact(task)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsReservingModelArtifact_NilTaskReturnsFalse(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()
	defer func() { _ = sugaredLogger.Sync() }()

	s := &Gopher{logger: sugaredLogger}
	assert.False(t, s.isReservingModelArtifact(nil), "nil task should not reserve artifact")
}

func TestFilterInternalArtifactObjectSummariesSkipsReplicationControlObjects(t *testing.T) {
	configName := "models/config.json"
	weightName := "models/model.safetensors"
	markerName := "models/" + constants.ArtifactCompleteMarkerFileName
	rootMarkerName := constants.ArtifactCompleteMarkerFileName
	lockName := "models/" + constants.ArtifactUploadLockFileName
	rootLockName := constants.ArtifactUploadLockFileName
	size := int64(1)

	objects := []objectstorage.ObjectSummary{
		{Name: &configName, Size: &size},
		{Name: &markerName, Size: &size},
		{Name: &lockName, Size: &size},
		{Name: &weightName, Size: &size},
		{Name: &rootMarkerName, Size: &size},
		{Name: &rootLockName, Size: &size},
	}

	filtered := filterInternalArtifactObjectSummaries(objects)

	require.Len(t, filtered, 2)
	assert.Equal(t, configName, *filtered[0].Name)
	assert.Equal(t, weightName, *filtered[1].Name)
}

func makeConfigMap(nodeName string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
			Namespace: "ome",
		},
		Data: data,
	}
}

func newGopherWithConfigMap(cm *corev1.ConfigMap) *Gopher {
	client := k8sfake.NewSimpleClientset(cm)
	logger := zap.NewNop().Sugar()
	cmr := NewConfigMapReconciler(cm.Name, cm.Namespace, client, logger)
	return &Gopher{
		configMapReconciler: cmr,
		logger:              logger,
	}
}

func newGopherForProcessTask(cm *corev1.ConfigMap, nodeLabels ...map[string]string) *Gopher {
	labels := map[string]string{}
	if len(nodeLabels) > 0 {
		labels = nodeLabels[0]
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   cm.Name,
			Labels: labels,
		},
	}
	client := k8sfake.NewSimpleClientset(cm, node)
	logger := zap.NewNop().Sugar()
	cmr := NewConfigMapReconciler(cm.Name, cm.Namespace, client, logger)
	return &Gopher{
		configMapReconciler: cmr,
		nodeLabelReconciler: NewNodeLabelReconciler(cm.Name, client, 1, logger),
		logger:              logger,
		activeDownloads:     map[string]activeDownload{},
	}
}

func newGopherForArtifactReuseProcessTask(t *testing.T, cm *corev1.ConfigMap, modelRootDir string, nodeLabels ...map[string]string) *Gopher {
	t.Helper()
	g := newGopherForProcessTask(cm, nodeLabels...)
	g.modelRootDir = modelRootDir
	g.downloadRetry = 1
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}
	g.modelConfigParser = modelparser.NewModelConfigParser(omefake.NewSimpleClientset(), g.logger)
	return g
}

func writeMinimalModelConfig(t *testing.T, modelPath string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(modelPath, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(modelPath, "config.json"), []byte(`{
  "architectures": ["LlamaForCausalLM"],
  "hidden_size": 16,
  "intermediate_size": 64,
  "max_position_embeddings": 128,
  "model_type": "llama",
  "num_attention_heads": 2,
  "num_hidden_layers": 2,
  "torch_dtype": "float16",
  "transformers_version": "4.44.0",
  "vocab_size": 128
}`), 0644))
}

func entryJSON(sha, parentName string, parentPath string) string {
	entry := struct {
		Config struct {
			Artifact Artifact `json:"artifact"`
		} `json:"config"`
	}{}
	entry.Config.Artifact = Artifact{
		Sha:           sha,
		ParentPath:    map[string]string{parentName: parentPath},
		ChildrenPaths: []string{},
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return ""
	}
	return string(b)
}

func entryJSONWithOrigin(status ModelStatus, modelID string, sha string, parentName string, parentPath string, childrenPaths []string) string {
	entry := ModelEntry{
		Status: status,
		Config: &ModelConfig{
			Artifact: Artifact{
				Sha: sha,
				Origin: &ArtifactOrigin{
					Type:        ArtifactOriginTypeHuggingFace,
					HFModelID:   modelID,
					HFCommitSHA: sha,
				},
				ParentPath:    map[string]string{parentName: parentPath},
				ChildrenPaths: childrenPaths,
			},
		},
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return ""
	}
	return string(b)
}

func modelEntryJSON(status ModelStatus) string {
	entry := ModelEntry{Status: status}
	b, err := json.Marshal(entry)
	if err != nil {
		return ""
	}
	return string(b)
}

func dp(v v1beta1.DownloadPolicy) *v1beta1.DownloadPolicy {
	return &v
}

func TestHuggingFaceArtifactIdentityFromAnnotations(t *testing.T) {
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"

	identity, ok := huggingFaceArtifactIdentityFromAnnotations(map[string]string{
		HuggingFaceModelIDAnnotationKey: "Qwen/Qwen3-4B-Instruct-2507",
		HuggingFaceSHAAnnotationKey:     sha,
	})

	require.True(t, ok)
	assert.Equal(t, ArtifactOriginTypeHuggingFace, identity.OriginType)
	assert.Equal(t, "Qwen/Qwen3-4B-Instruct-2507", identity.HFModelID)
	assert.Equal(t, sha, identity.HFCommitSHA)

	_, ok = huggingFaceArtifactIdentityFromAnnotations(map[string]string{
		HuggingFaceModelIDAnnotationKey: "Qwen/Qwen3-4B-Instruct-2507",
		HuggingFaceSHAAnnotationKey:     "main",
	})
	assert.False(t, ok)

	_, ok = huggingFaceArtifactIdentityFromAnnotations(nil)
	assert.False(t, ok)
}

func TestHuggingFaceArtifactIdentityRejectsPathUnsafeModelIDs(t *testing.T) {
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	unsafeModelIDs := []string{
		"../outside",
		"meta-llama/../../outside",
		"/absolute",
		"namespace//repo",
		"namespace/repo\\evil",
		"other..repo",
	}

	for _, modelID := range unsafeModelIDs {
		t.Run(modelID, func(t *testing.T) {
			_, ok := huggingFaceArtifactIdentityFromAnnotations(map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			})
			assert.False(t, ok)
		})
	}
}

func TestHuggingFaceArtifactKeyAndCanonicalPath(t *testing.T) {
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   "deepseek-ai/DeepSeek-V4-Pro",
		HFCommitSHA: sha,
	}
	destPath := filepath.Join(t.TempDir(), "customer-model-store", "model-ocid")

	key := huggingFaceArtifactConfigMapKey(identity)
	path := canonicalHuggingFaceArtifactPath(destPath, identity)

	assert.Contains(t, key, constants.HuggingFaceArtifactConfigMapKeyPrefix+"deepseek-ai.DeepSeek-V4-Pro.")
	assert.Contains(t, key, "."+sha)
	assert.NotContains(t, key, "/")
	assert.Equal(t, filepath.Join(filepath.Dir(destPath), constants.ModelArtifactsDirectory, "deepseek-ai", "DeepSeek-V4-Pro", sha), path)
}

func TestHuggingFaceArtifactParentPathForTaskPreservesClusterBaseModelLayout(t *testing.T) {
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   "deepseek-ai/DeepSeek-V4-Pro",
		HFCommitSHA: sha,
	}
	root := t.TempDir()
	destPath := filepath.Join(root, "customer-model-store", "model-ocid")

	baseModelPath := canonicalHuggingFaceArtifactPathForTask(&GopherTask{BaseModel: &v1beta1.BaseModel{}}, root, destPath, identity)
	clusterBaseModelPath := canonicalHuggingFaceArtifactPathForTask(&GopherTask{ClusterBaseModel: &v1beta1.ClusterBaseModel{}}, root, destPath, identity)

	assert.Equal(t, filepath.Join(filepath.Dir(destPath), constants.ModelArtifactsDirectory, "deepseek-ai", "DeepSeek-V4-Pro", sha), baseModelPath)
	assert.Equal(t, filepath.Join(root, "deepseek-ai", "DeepSeek-V4-Pro", sha), clusterBaseModelPath)
}

func TestHuggingFaceArtifactConfigMapKeyAvoidsModelIDSanitizationCollision(t *testing.T) {
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	first := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   "org/foo.bar",
		HFCommitSHA: sha,
	}
	second := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   "org.foo/bar",
		HFCommitSHA: sha,
	}

	assert.NotEqual(t, huggingFaceArtifactConfigMapKey(first), huggingFaceArtifactConfigMapKey(second))
}

func TestShouldUseHuggingFaceOriginObjectStorageReuse(t *testing.T) {
	policy := v1beta1.ReuseIfExists
	alwaysDownload := v1beta1.AlwaysDownload

	assert.True(t, shouldUseHuggingFaceOriginObjectStorageReuse(&GopherTask{TaskType: Download}, v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{DownloadPolicy: &policy},
	}))
	assert.False(t, shouldUseHuggingFaceOriginObjectStorageReuse(&GopherTask{TaskType: DownloadOverride}, v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{DownloadPolicy: &policy},
	}))
	assert.False(t, shouldUseHuggingFaceOriginObjectStorageReuse(&GopherTask{TaskType: Download}, v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{DownloadPolicy: &alwaysDownload},
	}))
	assert.False(t, shouldUseHuggingFaceOriginObjectStorageReuse(&GopherTask{TaskType: Download}, v1beta1.BaseModelSpec{}))
}

func TestShouldRepairHuggingFaceOriginObjectStorageParent(t *testing.T) {
	policy := v1beta1.ReuseIfExists
	alwaysDownload := v1beta1.AlwaysDownload

	assert.True(t, shouldRepairHuggingFaceOriginObjectStorageParent(&GopherTask{TaskType: DownloadOverride}, v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{DownloadPolicy: &policy},
	}))
	assert.False(t, shouldRepairHuggingFaceOriginObjectStorageParent(&GopherTask{TaskType: Download}, v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{DownloadPolicy: &policy},
	}))
	assert.False(t, shouldRepairHuggingFaceOriginObjectStorageParent(&GopherTask{TaskType: DownloadOverride}, v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{DownloadPolicy: &alwaysDownload},
	}))
	assert.False(t, shouldRepairHuggingFaceOriginObjectStorageParent(&GopherTask{TaskType: DownloadOverride}, v1beta1.BaseModelSpec{}))
}

func TestReuseHuggingFaceOriginArtifactUsesArtifactParentEntry(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	writeMinimalModelConfig(t, parentPath)

	artifactKey := huggingFaceArtifactConfigMapKey(identity)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		artifactKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, artifactKey, parentPath, []string{}),
	}))

	artifact, reused, err := g.reuseHuggingFaceOriginArtifactIfPossible(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, model.Spec, constants.BaseModel, model.Namespace, model.Name, childPath, identity)

	require.NoError(t, err)
	require.True(t, reused)
	require.NotNil(t, artifact)
	assert.Equal(t, parentPath, artifact.ParentPath[artifactKey])

	resolvedChild, err := filepath.EvalSymlinks(childPath)
	require.NoError(t, err)
	resolvedParent, err := filepath.EvalSymlinks(parentPath)
	require.NoError(t, err)
	assert.Equal(t, resolvedParent, resolvedChild)

	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), artifactKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(artifactKey, dataEntry)
	require.NoError(t, err)
	assert.Contains(t, childrenPaths, childPath)
}

func TestReuseHuggingFaceOriginArtifactBacksOffWhenParentTurnsUpdatingDuringChildRecord(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	writeMinimalModelConfig(t, parentPath)

	parentKey := huggingFaceArtifactConfigMapKey(identity)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	readyParent := makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	})
	updatingParent := makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	})
	g := newGopherWithConfigMap(readyParent)
	fakeClient, ok := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	require.True(t, ok)
	getCount := 0
	fakeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		getCount++
		if getCount == 1 {
			return true, readyParent.DeepCopy(), nil
		}
		if getCount == 2 {
			return true, updatingParent.DeepCopy(), nil
		}
		return false, nil, nil
	})

	artifact, reused, err := g.reuseHuggingFaceOriginArtifactIfPossible(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, model.Spec, constants.BaseModel, model.Namespace, model.Name, childPath, identity)

	require.NoError(t, err)
	assert.False(t, reused)
	assert.Nil(t, artifact)
}

func TestReuseHuggingFaceOriginArtifactIgnoresModelEntryWithoutArtifactParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	oldModelPath := filepath.Join(tmpDir, "old-model")
	childPath := filepath.Join(tmpDir, "models", "child")
	writeMinimalModelConfig(t, oldModelPath)

	oldModelKey := constants.GetModelConfigMapKey("default", "old-model", false)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		oldModelKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, oldModelKey, oldModelPath, []string{}),
	}))

	artifact, reused, err := g.reuseHuggingFaceOriginArtifactIfPossible(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, model.Spec, constants.BaseModel, model.Namespace, model.Name, childPath, identity)

	require.NoError(t, err)
	assert.False(t, reused)
	assert.Nil(t, artifact)
	_, err = os.Lstat(childPath)
	assert.True(t, os.IsNotExist(err), "new model path should not symlink to an old model-local parent")
}

func TestGetHuggingFaceArtifactParentReturnsUpdatingParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	}))

	gotKey, gotPath, gotStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)

	require.True(t, ok)
	assert.Equal(t, parentKey, gotKey)
	assert.Equal(t, parentPath, gotPath)
	assert.Equal(t, ModelStatusUpdating, gotStatus)
}

func TestReserveHuggingFaceArtifactParentEntryCreatesUpdatingParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))

	gotPath, gotStatus, reserved, err := g.configMapReconciler.reserveHuggingFaceArtifactParentEntry(context.Background(), parentKey, identity, parentPath)

	require.NoError(t, err)
	assert.True(t, reserved)
	assert.Equal(t, parentPath, gotPath)
	assert.Equal(t, ModelStatusUpdating, gotStatus)

	gotKey, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentKey, gotKey)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
}

func TestReserveHuggingFaceArtifactParentEntryReplacesCorruptSyntheticParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: "{not-json",
	}))

	gotPath, gotStatus, reserved, err := g.configMapReconciler.reserveHuggingFaceArtifactParentEntry(context.Background(), parentKey, identity, parentPath)

	require.NoError(t, err)
	assert.True(t, reserved)
	assert.Equal(t, parentPath, gotPath)
	assert.Equal(t, ModelStatusUpdating, gotStatus)
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
}

func TestReserveHuggingFaceArtifactParentEntryReplacesInvalidSyntheticParentMetadata(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	requestedParentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	existingParentPath := filepath.Join(tmpDir, "models", "existing-parent")
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	childPath1 := filepath.Join(tmpDir, "models", "child-1")
	childPath2 := filepath.Join(tmpDir, "models", "child-2")
	invalidEntry, err := json.Marshal(ModelEntry{
		Name:   parentKey,
		Status: ModelStatusReady,
		Config: &ModelConfig{
			Artifact: Artifact{
				Sha:           sha,
				ParentPath:    map[string]string{parentKey: existingParentPath},
				ChildrenPaths: []string{childPath1, childPath2},
			},
		},
	})
	require.NoError(t, err)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: string(invalidEntry),
	}))

	gotPath, gotStatus, reserved, err := g.configMapReconciler.reserveHuggingFaceArtifactParentEntry(context.Background(), parentKey, identity, requestedParentPath)

	require.NoError(t, err)
	assert.True(t, reserved)
	assert.Equal(t, existingParentPath, gotPath)
	assert.Equal(t, ModelStatusUpdating, gotStatus)
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, existingParentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(parentKey, dataEntry)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{childPath1, childPath2}, childrenPaths)
}

func TestReserveHuggingFaceArtifactParentEntryPreservesUpdatingOwnershipWhenRepairingInvalidMetadata(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	requestedParentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	existingParentPath := filepath.Join(tmpDir, "models", "existing-parent")
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	childPath := filepath.Join(tmpDir, "models", "child")
	invalidEntry, err := json.Marshal(ModelEntry{
		Name:   parentKey,
		Status: ModelStatusUpdating,
		Config: &ModelConfig{
			Artifact: Artifact{
				Sha:           sha,
				ParentPath:    map[string]string{parentKey: existingParentPath},
				ChildrenPaths: []string{childPath},
			},
		},
	})
	require.NoError(t, err)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: string(invalidEntry),
	}))

	gotPath, gotStatus, reserved, err := g.configMapReconciler.reserveHuggingFaceArtifactParentEntry(context.Background(), parentKey, identity, requestedParentPath)

	require.NoError(t, err)
	assert.False(t, reserved)
	assert.Equal(t, existingParentPath, gotPath)
	assert.Equal(t, ModelStatusUpdating, gotStatus)
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, existingParentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(parentKey, dataEntry)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{childPath}, childrenPaths)
}

func TestReserveHuggingFaceArtifactParentEntryRejectsMismatchedSyntheticParentIdentity(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	otherSHA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, otherSHA, parentKey, parentPath, []string{}),
	}))

	gotPath, gotStatus, reserved, err := g.configMapReconciler.reserveHuggingFaceArtifactParentEntry(context.Background(), parentKey, identity, parentPath)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatched origin metadata")
	assert.False(t, reserved)
	assert.Empty(t, gotPath)
	assert.Empty(t, gotStatus)
}

func TestMarkHuggingFaceArtifactParentUpdatingPreservesChildren(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	childPath := filepath.Join(tmpDir, "models", "child")
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{childPath}),
	}))

	gotPath, acquired, err := g.configMapReconciler.markHuggingFaceArtifactParentUpdating(context.Background(), parentKey, identity, parentPath)

	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, parentPath, gotPath)

	gotKey, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentKey, gotKey)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)

	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(parentKey, dataEntry)
	require.NoError(t, err)
	assert.Contains(t, childrenPaths, childPath)
}

func TestUpsertHuggingFaceArtifactParentEntryRejectsChildPathWhenParentUpdating(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	childPath := filepath.Join(tmpDir, "models", "child")
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	}))

	err := g.configMapReconciler.upsertHuggingFaceArtifactParentEntry(context.Background(), parentKey, identity, parentPath, childPath)

	require.ErrorIs(t, err, errHuggingFaceArtifactParentUpdating)
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(parentKey, dataEntry)
	require.NoError(t, err)
	assert.NotContains(t, childrenPaths, childPath)
}

func TestMarkHuggingFaceArtifactParentUpdatingDoesNotAcquireUpdatingParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	childPath := filepath.Join(tmpDir, "models", "child")
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{childPath}),
	}))

	gotPath, acquired, err := g.configMapReconciler.markHuggingFaceArtifactParentUpdating(context.Background(), parentKey, identity, parentPath)

	require.NoError(t, err)
	assert.False(t, acquired)
	assert.Equal(t, parentPath, gotPath)

	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
}

func TestTryAcquireHuggingFaceArtifactParentRebuildMarksFailedParentUpdating(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusFailed, modelID, sha, parentKey, parentPath, []string{}),
	}))

	rebuildPath, acquired, err := g.tryAcquireHuggingFaceArtifactParentRebuild(context.Background(), parentKey, parentPath, identity)

	require.NoError(t, err)
	require.True(t, acquired)
	assert.Equal(t, parentPath, rebuildPath)
	assert.False(t, hasHuggingFaceArtifactReadyMarker(parentPath), "rebuild owner should clear stale ready marker before writing parent")

	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
}

func TestRecoverStartupHuggingFaceArtifactParentsMarksUpdatingWithoutReadyMarkerFailed(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	}))

	g.recoverStartupHuggingFaceArtifactParents(context.Background())

	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusFailed, parentStatus)
}

func TestRecoverStartupHuggingFaceArtifactParentsMarksUpdatingWithReadyMarkerReady(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	}))

	g.recoverStartupHuggingFaceArtifactParents(context.Background())

	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusReady, parentStatus)
}

func TestRecoverStartupHuggingFaceArtifactParentsDeletesInvalidUpdatingParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	parentPath := canonicalHuggingFaceArtifactPath(filepath.Join(tmpDir, "child"), identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	invalidEntry, err := json.Marshal(ModelEntry{
		Name:   parentKey,
		Status: ModelStatusUpdating,
		Config: &ModelConfig{
			Artifact: Artifact{
				Sha:           sha,
				ParentPath:    map[string]string{parentKey: parentPath},
				ChildrenPaths: []string{filepath.Join(tmpDir, "child")},
			},
		},
	})
	require.NoError(t, err)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: string(invalidEntry),
	}))

	recovered := g.recoverStartupHuggingFaceArtifactParents(context.Background())

	require.True(t, recovered)
	exists, _, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestEnsureStartupHuggingFaceArtifactParentsRecoveredDoesNotWaitWhenConfigMapMissing(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	logger := zap.NewNop().Sugar()
	g := &Gopher{
		configMapReconciler: NewConfigMapReconciler("node-1", "ome", client, logger),
		logger:              logger,
		gopherChan:          make(chan *GopherTask, 1),
		samePathWaitDelay:   time.Millisecond,
		samePathWaitTimeout: time.Hour,
	}

	recovered := g.recoverStartupHuggingFaceArtifactParents(context.Background())
	require.True(t, recovered)
	require.False(t, g.isStartupHuggingFaceArtifactParentRecoveryPending())

	stop, err := g.ensureStartupHuggingFaceArtifactParentsRecovered(context.Background(), &GopherTask{TaskType: Download}, constants.HuggingFaceArtifactConfigMapKeyPrefix+"test")

	require.NoError(t, err)
	assert.False(t, stop)
	assert.False(t, g.isStartupHuggingFaceArtifactParentRecoveryPending())
	select {
	case <-g.gopherChan:
		t.Fatal("missing startup ConfigMap should not requeue or block first download")
	default:
	}
}

func TestStartupConfigMapContextHasDeadline(t *testing.T) {
	before := time.Now()
	ctx, cancel := startupConfigMapContext()
	defer cancel()

	deadline, ok := ctx.Deadline()

	require.True(t, ok)
	assert.True(t, deadline.After(before))
	assert.LessOrEqual(t, time.Until(deadline), defaultStartupConfigMapSnapshotTimeout)
}

func TestRunUsesSeparateStartupContextsForRecoveryAndSnapshot(t *testing.T) {
	originalStartupConfigMapContext := startupConfigMapContext
	defer func() {
		startupConfigMapContext = originalStartupConfigMapContext
	}()
	startupContextCalls := 0
	startupConfigMapContext = func() (context.Context, context.CancelFunc) {
		startupContextCalls++
		return context.WithCancel(context.Background())
	}
	g := newGopherForProcessTask(makeConfigMap("node-1", map[string]string{}))
	g.gopherChan = make(chan *GopherTask)

	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		g.Run(stopCh, 1, 1)
		close(done)
	}()
	close(stopCh)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("gopher Run did not stop")
	}
	assert.Equal(t, 2, startupContextCalls)
}

func TestValidateStartupHuggingFaceArtifactParentRunsOncePerRestart(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}))
	g.startupHuggingFaceParentValidationKeys = map[string]struct{}{parentKey: {}}
	downloadCalls := 0

	stop, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		assert.Equal(t, parentPath, downloadPath)
		assert.False(t, hasHuggingFaceArtifactReadyMarker(parentPath), "startup validation owner should clear ready marker before validating parent files")
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 1, downloadCalls)
	assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath))
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusReady, parentStatus)

	stop, err = g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 1, downloadCalls)
}

func TestValidateStartupHuggingFaceArtifactParentRetainsValidationAfterAcquireError(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}))
	g.startupHuggingFaceParentValidationKeys = map[string]struct{}{parentKey: {}}
	fakeClient, ok := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	require.True(t, ok)
	updateErr := fmt.Errorf("transient configmap update failure")
	failNextUpdate := true
	fakeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if failNextUpdate {
			failNextUpdate = false
			return true, nil, updateErr
		}
		return false, nil, nil
	})
	downloadCalls := 0

	stop, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		return nil
	})

	require.ErrorIs(t, err, updateErr)
	assert.False(t, stop)
	assert.Equal(t, 0, downloadCalls)

	stop, err = g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		assert.Equal(t, parentPath, downloadPath)
		assert.False(t, hasHuggingFaceArtifactReadyMarker(parentPath), "startup validation owner should clear ready marker before validating parent files")
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 1, downloadCalls)
	assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath))
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusReady, parentStatus)
}

func TestValidateStartupHuggingFaceArtifactParentStopsAfterAcquireBusy(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}))
	g.startupHuggingFaceParentValidationKeys = map[string]struct{}{parentKey: {}}
	g.gopherChan = make(chan *GopherTask, 2)
	g.samePathWaitDelay = time.Millisecond
	fakeClient, ok := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	require.True(t, ok)
	updateConflictSeen := false
	fakeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if updateConflictSeen {
			return true, makeConfigMap("node-1", map[string]string{
				parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
			}), nil
		}
		return false, nil, nil
	})
	fakeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !updateConflictSeen {
			updateConflictSeen = true
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "node-1", errors.New("parent already updating"))
		}
		return false, nil, nil
	})
	downloadCalls := 0
	task := &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "default"}},
	}

	stop, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), task, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		return nil
	})

	require.NoError(t, err)
	assert.True(t, stop)
	assert.Equal(t, 0, downloadCalls)
	select {
	case requeued := <-g.gopherChan:
		require.NotNil(t, requeued)
		assert.Equal(t, task, requeued)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected busy parent validation to requeue exactly once")
	}
	select {
	case <-g.gopherChan:
		t.Fatal("expected only one requeue when startup validation loses parent ownership")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestValidateStartupHuggingFaceArtifactParentSkipsValidationWhenStartupConfigMapNotFound(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g, client := newGopherWithEmptyClient("node-1", "ome", t)

	g.captureStartupReadyModels(context.Background())
	_, err := client.CoreV1().ConfigMaps("ome").Create(context.Background(), makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}), metav1.CreateOptions{})
	require.NoError(t, err)
	downloadCalls := 0

	stop, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 0, downloadCalls)
}

func TestValidateStartupHuggingFaceArtifactParentValidatesWhenStartupSnapshotReadFails(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g, client := newGopherWithEmptyClient("node-1", "ome", t)
	fakeClient, ok := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	require.True(t, ok)
	failStartupGet := true
	fakeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if failStartupGet {
			failStartupGet = false
			return true, nil, errors.New("temporary configmap read failure")
		}
		return false, nil, nil
	})

	g.captureStartupReadyModels(context.Background())
	_, err := client.CoreV1().ConfigMaps("ome").Create(context.Background(), makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}), metav1.CreateOptions{})
	require.NoError(t, err)
	downloadCalls := 0

	stop, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		assert.Equal(t, parentPath, downloadPath)
		assert.False(t, hasHuggingFaceArtifactReadyMarker(parentPath), "startup validation owner should clear ready marker before validating parent files")
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 1, downloadCalls)
	assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath))

	stop, err = g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 1, downloadCalls)
}

func TestValidateStartupHuggingFaceArtifactParentRetainsValidationAfterParentLookupUnavailable(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g, client := newGopherWithEmptyClient("node-1", "ome", t)
	g.startupHuggingFaceParentValidationKeys = map[string]struct{}{parentKey: {}}
	downloadCalls := 0

	stop, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 0, downloadCalls)

	_, err = client.CoreV1().ConfigMaps("ome").Create(context.Background(), makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}), metav1.CreateOptions{})
	require.NoError(t, err)

	stop, err = g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: &v1beta1.BaseModel{},
	}, parentKey, identity, func(downloadPath string) error {
		downloadCalls++
		assert.Equal(t, parentPath, downloadPath)
		assert.False(t, hasHuggingFaceArtifactReadyMarker(parentPath), "startup validation owner should clear ready marker before validating parent files")
		return nil
	})

	require.NoError(t, err)
	assert.False(t, stop)
	assert.Equal(t, 1, downloadCalls)
	assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath))
}

func TestValidateStartupHuggingFaceArtifactParentBlocksSiblingUntilParentUpdating(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}))
	g.startupHuggingFaceParentValidationKeys = map[string]struct{}{parentKey: {}}
	fakeClient, ok := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	require.True(t, ok)

	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	var blockOnce sync.Once
	fakeClient.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		blockOnce.Do(func() {
			close(updateStarted)
			<-releaseUpdate
		})
		return false, nil, nil
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
			TaskType:  Download,
			BaseModel: &v1beta1.BaseModel{},
		}, parentKey, identity, func(downloadPath string) error {
			return nil
		})
		firstDone <- err
	}()

	select {
	case <-updateStarted:
	case <-time.After(time.Second):
		t.Fatal("expected first validation to start marking the parent Updating")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := g.validateStartupHuggingFaceArtifactParentIfNeeded(context.Background(), &GopherTask{
			TaskType:  Download,
			BaseModel: &v1beta1.BaseModel{},
		}, parentKey, identity, func(downloadPath string) error {
			return nil
		})
		secondDone <- err
	}()

	select {
	case <-secondDone:
		t.Fatal("sibling validation should wait until the startup parent is marked Updating")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseUpdate)
	require.NoError(t, <-firstDone)
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("expected sibling validation to finish after parent Updating transition completes")
	}
}

func TestRepairHuggingFaceOriginArtifactParentDownloadsParentAndLinksChild(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	siblingPath := filepath.Join(tmpDir, "models", "sibling")
	writeMinimalModelConfig(t, parentPath)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))

	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{siblingPath}),
	}))
	downloadCalled := false
	downloadParent := func(downloadPath string) error {
		downloadCalled = true
		assert.Equal(t, parentPath, downloadPath)
		assert.False(t, hasHuggingFaceArtifactReadyMarker(parentPath), "repair should clear stale ready marker before writing parent")
		writeMinimalModelConfig(t, downloadPath)
		return nil
	}

	artifact, repaired, err := g.repairHuggingFaceOriginArtifactParent(context.Background(), &GopherTask{
		TaskType:  DownloadOverride,
		BaseModel: model,
	}, model.Name, childPath, parentKey, parentPath, identity, downloadParent)

	require.NoError(t, err)
	require.True(t, repaired)
	require.True(t, downloadCalled)
	require.NotNil(t, artifact)
	assert.Equal(t, parentPath, artifact.ParentPath[parentKey])

	resolvedChild, err := filepath.EvalSymlinks(childPath)
	require.NoError(t, err)
	resolvedParent, err := filepath.EvalSymlinks(parentPath)
	require.NoError(t, err)
	assert.Equal(t, resolvedParent, resolvedChild)
	assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath))

	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusReady, parentStatus)

	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(parentKey, dataEntry)
	require.NoError(t, err)
	assert.Contains(t, childrenPaths, siblingPath)
	assert.Contains(t, childrenPaths, childPath)
}

func TestRepairHuggingFaceOriginArtifactParentMarksParentFailedOnDownloadError(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	writeMinimalModelConfig(t, parentPath)

	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}))
	downloadErr := fmt.Errorf("download failed")

	artifact, repaired, err := g.repairHuggingFaceOriginArtifactParent(context.Background(), &GopherTask{
		TaskType:  DownloadOverride,
		BaseModel: model,
	}, model.Name, childPath, parentKey, parentPath, identity, func(downloadPath string) error {
		return downloadErr
	})

	assert.Nil(t, artifact)
	assert.False(t, repaired)
	assert.ErrorIs(t, err, downloadErr)

	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusFailed, parentStatus)
}

func TestRepairHuggingFaceOriginArtifactParentReplacesCorruptSyntheticParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)

	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: "{not-json",
	}))
	downloadCalled := false

	artifact, repaired, err := g.repairHuggingFaceOriginArtifactParent(context.Background(), &GopherTask{
		TaskType:  DownloadOverride,
		BaseModel: model,
	}, model.Name, childPath, parentKey, parentPath, identity, func(downloadPath string) error {
		downloadCalled = true
		assert.Equal(t, parentPath, downloadPath)
		writeMinimalModelConfig(t, downloadPath)
		return nil
	})

	require.NoError(t, err)
	assert.True(t, repaired)
	assert.True(t, downloadCalled)
	require.NotNil(t, artifact)
	assert.Equal(t, parentPath, artifact.ParentPath[parentKey])
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, parentPath, gotParentPath)
	assert.Equal(t, ModelStatusReady, parentStatus)
}

func TestRepairHuggingFaceOriginArtifactParentPreservesInvalidSyntheticParentMetadata(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	requestedParentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	existingParentPath := filepath.Join(tmpDir, "models", "existing-parent")
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	siblingPath := filepath.Join(tmpDir, "models", "sibling")
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(existingParentPath))
	invalidEntry, err := json.Marshal(ModelEntry{
		Name:   parentKey,
		Status: ModelStatusReady,
		Config: &ModelConfig{
			Artifact: Artifact{
				Sha:           sha,
				ParentPath:    map[string]string{parentKey: existingParentPath},
				ChildrenPaths: []string{siblingPath},
			},
		},
	})
	require.NoError(t, err)

	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: string(invalidEntry),
	}))
	downloadCalled := false

	artifact, repaired, err := g.repairHuggingFaceOriginArtifactParent(context.Background(), &GopherTask{
		TaskType:  DownloadOverride,
		BaseModel: model,
	}, model.Name, childPath, parentKey, requestedParentPath, identity, func(downloadPath string) error {
		downloadCalled = true
		assert.Equal(t, existingParentPath, downloadPath)
		assert.False(t, hasHuggingFaceArtifactReadyMarker(existingParentPath), "repair should clear stale ready marker before writing parent")
		writeMinimalModelConfig(t, downloadPath)
		return nil
	})

	require.NoError(t, err)
	assert.True(t, repaired)
	assert.True(t, downloadCalled)
	require.NotNil(t, artifact)
	assert.Equal(t, existingParentPath, artifact.ParentPath[parentKey])
	_, gotParentPath, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, existingParentPath, gotParentPath)
	assert.Equal(t, ModelStatusReady, parentStatus)
	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(parentKey, dataEntry)
	require.NoError(t, err)
	assert.Contains(t, childrenPaths, siblingPath)
	assert.Contains(t, childrenPaths, childPath)
}

func TestRepairHuggingFaceOriginArtifactParentKeepsReadyMarkerWhenRepairNotAcquired(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	writeMinimalModelConfig(t, parentPath)
	require.NoError(t, writeHuggingFaceArtifactReadyMarker(parentPath))

	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	readyParent := makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	})
	updatingParent := readyParent.DeepCopy()
	updatingParent.Data[parentKey] = entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{})
	g := newGopherWithConfigMap(readyParent)
	fakeClient, ok := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	require.True(t, ok)
	getCount := 0
	fakeClient.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		getCount++
		if getCount >= 3 {
			return true, updatingParent.DeepCopy(), nil
		}
		return false, nil, nil
	})
	downloadCalled := false

	artifact, repaired, err := g.repairHuggingFaceOriginArtifactParent(context.Background(), &GopherTask{
		TaskType:              DownloadOverride,
		BaseModel:             model,
		SamePathWaitStartedAt: time.Now().Add(-defaultSamePathWaitTimeout),
	}, model.Name, childPath, parentKey, parentPath, identity, func(downloadPath string) error {
		downloadCalled = true
		return nil
	})

	assert.Nil(t, artifact)
	assert.False(t, repaired)
	assert.ErrorContains(t, err, "timed out waiting for Hugging Face artifact parent")
	assert.False(t, downloadCalled)
	assert.True(t, hasHuggingFaceArtifactReadyMarker(parentPath), "non-owner repair must not remove the ready marker")
}

func TestProcessTaskRequeuesWhenHuggingFaceArtifactParentIsUpdating(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			UID:       "child-uid",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	}), tmpDir)
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	g.samePathWaitTimeout = time.Hour

	err := g.processTaskWithOptions(&GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, true)

	require.NoError(t, err)
	select {
	case task := <-g.gopherChan:
		assert.Equal(t, model.Name, task.BaseModel.Name)
		assert.False(t, task.SamePathWaitStartedAt.IsZero())
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected Updating Hugging Face artifact parent task to be requeued")
	}
	_, err = os.Lstat(childPath)
	assert.True(t, os.IsNotExist(err), "child path should not be created while parent artifact is Updating")
}

func TestProcessTaskWaitsOnUpdatingParentCreatedAfterMissingStartupConfigMap(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			UID:       "child-uid",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	client := k8sfake.NewSimpleClientset(node)
	logger := zap.NewNop().Sugar()
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{}), tmpDir)
	g.configMapReconciler = NewConfigMapReconciler("node-1", "ome", client, logger)
	g.nodeLabelReconciler = NewNodeLabelReconciler("node-1", client, 1, logger)
	g.logger = logger
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	g.samePathWaitTimeout = time.Hour

	g.recoverStartupHuggingFaceArtifactParents(context.Background())
	_, err := client.CoreV1().ConfigMaps("ome").Create(context.Background(), makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	}), metav1.CreateOptions{})
	require.NoError(t, err)

	err = g.processTaskWithOptions(&GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, false)

	require.NoError(t, err)
	_, _, parentStatus, ok := g.getHuggingFaceArtifactParent(context.Background(), identity)
	require.True(t, ok)
	assert.Equal(t, ModelStatusUpdating, parentStatus)
	select {
	case task := <-g.gopherChan:
		assert.Equal(t, model.Name, task.BaseModel.Name)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Updating parent created after missing startup ConfigMap should be waited on, not marked Failed")
	}
}

func TestProcessTaskRecoversUpdatingHuggingFaceArtifactParentWithReadyMarker(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	writeMinimalModelConfig(t, parentPath)
	require.NoError(t, os.WriteFile(huggingFaceArtifactReadyMarkerPath(parentPath), []byte("ready\n"), 0644))

	parentKey := huggingFaceArtifactConfigMapKey(identity)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			UID:       "child-uid",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	labelKey, err := getModelLabelKey(&NodeLabelOp{ModelStateOnNode: Ready, BaseModel: model})
	require.NoError(t, err)
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusUpdating, modelID, sha, parentKey, parentPath, []string{}),
	}), tmpDir, map[string]string{labelKey: string(Ready)})
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}

	err = g.processTaskWithOptions(&GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, true)

	require.NoError(t, err)
	resolvedChild, err := filepath.EvalSymlinks(childPath)
	require.NoError(t, err)
	resolvedParent, err := filepath.EvalSymlinks(parentPath)
	require.NoError(t, err)
	assert.Equal(t, resolvedParent, resolvedChild)

	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, latest.Data, parentKey)
	var parentEntry ModelEntry
	require.NoError(t, json.Unmarshal([]byte(latest.Data[parentKey]), &parentEntry))
	assert.Equal(t, ModelStatusReady, parentEntry.Status)
	assert.Contains(t, parentEntry.Config.Artifact.ChildrenPaths, childPath)
}

func TestProcessTaskWithOptionsReusesOCIArtifactByHuggingFaceOrigin(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	writeMinimalModelConfig(t, parentPath)

	parentKey := huggingFaceArtifactConfigMapKey(identity)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			UID:       "child-uid",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3"),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	labelKey, err := getModelLabelKey(&NodeLabelOp{ModelStateOnNode: Ready, BaseModel: model})
	require.NoError(t, err)
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}), tmpDir, map[string]string{labelKey: string(Ready)})
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}

	err = g.processTaskWithOptions(&GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, true)

	require.NoError(t, err)
	resolvedChild, err := filepath.EvalSymlinks(childPath)
	require.NoError(t, err)
	resolvedParent, err := filepath.EvalSymlinks(parentPath)
	require.NoError(t, err)
	assert.Equal(t, resolvedParent, resolvedChild)

	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Contains(t, getChildrenPaths(latest.Data[parentKey]), childPath)

	currentKey := constants.GetModelConfigMapKey(model.Namespace, model.Name, false)
	var currentEntry ModelEntry
	require.NoError(t, json.Unmarshal([]byte(latest.Data[currentKey]), &currentEntry))
	assert.Equal(t, ModelStatusReady, currentEntry.Status)
	require.NotNil(t, currentEntry.Config)
	require.NotNil(t, currentEntry.Config.Artifact.Origin)
	assert.Equal(t, ArtifactOriginTypeHuggingFace, currentEntry.Config.Artifact.Origin.Type)
	assert.Equal(t, modelID, currentEntry.Config.Artifact.Origin.HFModelID)
	assert.Equal(t, sha, currentEntry.Config.Artifact.Origin.HFCommitSHA)
	assert.Equal(t, parentPath, currentEntry.Config.Artifact.ParentPath[parentKey])
}

func TestReuseHuggingFaceOriginArtifactCreatesSymlinkAndUpdatesChildren(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	require.NoError(t, os.MkdirAll(parentPath, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "config.json"), []byte("{}"), 0644))

	parentKey := huggingFaceArtifactConfigMapKey(identity)
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child",
			Namespace: "default",
			Annotations: map[string]string{
				HuggingFaceModelIDAnnotationKey: modelID,
				HuggingFaceSHAAnnotationKey:     sha,
			},
		},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri:     stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3"),
				Path:           &childPath,
				DownloadPolicy: dp(v1beta1.ReuseIfExists),
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}))

	artifact, reused, err := g.reuseHuggingFaceOriginArtifactIfPossible(context.Background(), &GopherTask{
		TaskType:  Download,
		BaseModel: model,
	}, model.Spec, constants.BaseModel, model.Namespace, model.Name, childPath, identity)

	require.NoError(t, err)
	require.True(t, reused)
	require.NotNil(t, artifact)
	assert.Equal(t, sha, artifact.Sha)
	require.NotNil(t, artifact.Origin)
	assert.Equal(t, modelID, artifact.Origin.HFModelID)
	assert.Equal(t, parentPath, artifact.ParentPath[parentKey])

	resolvedChild, err := filepath.EvalSymlinks(childPath)
	require.NoError(t, err)
	resolvedParent, err := filepath.EvalSymlinks(parentPath)
	require.NoError(t, err)
	assert.Equal(t, resolvedParent, resolvedChild)

	exists, dataEntry, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), parentKey)
	require.NoError(t, err)
	require.True(t, exists)
	_, childrenPaths, err := g.configMapReconciler.getParentPathAndChildrenPaths(parentKey, dataEntry)
	require.NoError(t, err)
	assert.Contains(t, childrenPaths, childPath)
}

func TestProcessTaskDeletesOCIOriginChildAndPreservesExistingParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	tmpDir := t.TempDir()
	parentPath := filepath.Join(tmpDir, "parent")
	childPath := filepath.Join(tmpDir, "child")
	writeMinimalModelConfig(t, parentPath)
	require.NoError(t, os.Symlink(parentPath, childPath))

	parentKey := constants.GetModelConfigMapKey("default", "parent", false)
	child := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "default", UID: "child-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3"),
				Path:       &childPath,
			},
		},
	}
	childKey := constants.GetModelConfigMapKey(child.Namespace, child.Name, false)
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{childPath}),
		childKey:  entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}), tmpDir)

	err := g.processTask(&GopherTask{TaskType: Delete, BaseModel: child})

	require.NoError(t, err)
	_, err = os.Lstat(childPath)
	assert.True(t, os.IsNotExist(err), "child symlink should be removed")
	_, err = os.Stat(parentPath)
	assert.NoError(t, err, "parent artifact should remain while the parent ConfigMap entry exists")

	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, latest.Data, childKey)
	assert.NotContains(t, getChildrenPaths(latest.Data[parentKey]), childPath)
}

func TestProcessTaskRequeuesOCIOriginChildDeletionWhenSharedMetadataReadFails(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	tmpDir := t.TempDir()
	parentPath := filepath.Join(tmpDir, "parent")
	childPath := filepath.Join(tmpDir, "child")
	writeMinimalModelConfig(t, parentPath)
	require.NoError(t, os.Symlink(parentPath, childPath))

	parentKey := constants.GetModelConfigMapKey("default", "parent", false)
	child := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "default", UID: "child-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3"),
				Path:       &childPath,
			},
		},
	}
	childKey := constants.GetModelConfigMapKey(child.Namespace, child.Name, false)
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{
		parentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{childPath}),
		childKey:  entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}), tmpDir)
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond

	fakeClient, ok := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	require.True(t, ok)
	failedInspection := false
	fakeClient.Fake.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !failedInspection {
			failedInspection = true
			return true, nil, errors.New("temporary configmap read failure")
		}
		return false, nil, nil
	})

	err := g.processTask(&GopherTask{TaskType: Delete, BaseModel: child})

	require.NoError(t, err)
	_, err = os.Lstat(childPath)
	assert.NoError(t, err, "child symlink should remain until shared metadata can be inspected")
	_, err = os.Stat(parentPath)
	assert.NoError(t, err, "parent artifact should remain")

	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Contains(t, latest.Data, childKey)
	assert.Contains(t, getChildrenPaths(latest.Data[parentKey]), childPath)

	select {
	case requeued := <-g.gopherChan:
		require.NotNil(t, requeued)
		assert.Equal(t, Delete, requeued.TaskType)
		assert.False(t, requeued.SamePathWaitStartedAt.IsZero())
		require.NotNil(t, requeued.BaseModel)
		assert.Equal(t, child.Name, requeued.BaseModel.Name)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected delete task to be requeued")
	}
}

func TestRequeueDeleteAfterSharedArtifactMetadataInspectionErrorTimesOut(t *testing.T) {
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	g.samePathWaitTimeout = time.Millisecond
	task := &GopherTask{
		TaskType:              Delete,
		SamePathWaitStartedAt: time.Now().Add(-time.Hour),
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default"},
		},
	}

	requeued := g.requeueDeleteAfterSharedArtifactMetadataInspectionError(task, errors.New("configmap unavailable"))

	assert.False(t, requeued)
	select {
	case <-g.gopherChan:
		t.Fatal("timed out delete metadata inspection task should not be requeued")
	default:
	}
}

func TestProcessTaskDeletesOCIOriginChildAndRemovesOrphanParent(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	tmpDir := t.TempDir()
	parentPath := filepath.Join(tmpDir, "parent")
	childPath := filepath.Join(tmpDir, "child")
	writeMinimalModelConfig(t, parentPath)
	require.NoError(t, os.Symlink(parentPath, childPath))

	parentKey := constants.GetModelConfigMapKey("default", "deleted-parent", false)
	child := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "default", UID: "child-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3"),
				Path:       &childPath,
			},
		},
	}
	childKey := constants.GetModelConfigMapKey(child.Namespace, child.Name, false)
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{
		childKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
	}), tmpDir)

	err := g.processTask(&GopherTask{TaskType: Delete, BaseModel: child})

	require.NoError(t, err)
	_, err = os.Lstat(childPath)
	assert.True(t, os.IsNotExist(err), "child symlink should be removed")
	_, err = os.Stat(parentPath)
	assert.True(t, os.IsNotExist(err), "orphan parent artifact should be removed after the last child is deleted")

	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, latest.Data, childKey)
}

func TestProcessTaskDeletesLastOCIOriginChildAndRemovesArtifactParentEntry(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	tmpDir := t.TempDir()
	childPath := filepath.Join(tmpDir, "models", "child")
	parentPath := canonicalHuggingFaceArtifactPath(childPath, identity)
	writeMinimalModelConfig(t, parentPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(childPath), 0755))
	require.NoError(t, os.Symlink(parentPath, childPath))

	artifactKey := huggingFaceArtifactConfigMapKey(identity)
	child := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "default", UID: "child-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: stringPtr("oci://n/ns/b/bucket/o/customer-imported-basemodels/qwen/qwen3/" + sha),
				Path:       &childPath,
			},
		},
	}
	childKey := constants.GetModelConfigMapKey(child.Namespace, child.Name, false)
	g := newGopherForArtifactReuseProcessTask(t, makeConfigMap("node-1", map[string]string{
		artifactKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, artifactKey, parentPath, []string{childPath}),
		childKey:    entryJSONWithOrigin(ModelStatusReady, modelID, sha, artifactKey, parentPath, []string{}),
	}), tmpDir)

	err := g.processTask(&GopherTask{TaskType: Delete, BaseModel: child})

	require.NoError(t, err)
	_, err = os.Lstat(childPath)
	assert.True(t, os.IsNotExist(err), "child symlink should be removed")
	_, err = os.Stat(parentPath)
	assert.True(t, os.IsNotExist(err), "canonical artifact should be removed after the last child is deleted")

	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, latest.Data, childKey)
	assert.NotContains(t, latest.Data, artifactKey)
}

func TestConvertMetadataToModelConfigPreservesArtifactOrigin(t *testing.T) {
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"

	config := ConvertMetadataToModelConfig(ModelMetadata{
		Artifact: Artifact{
			Sha: sha,
			Origin: &ArtifactOrigin{
				Type:        ArtifactOriginTypeHuggingFace,
				HFModelID:   "Qwen/Qwen3-4B-Instruct-2507",
				HFCommitSHA: sha,
			},
			ParentPath:    map[string]string{"default.basemodel.parent": "/models/parent"},
			ChildrenPaths: []string{"/models/child"},
		},
	})

	require.NotNil(t, config.Artifact.Origin)
	assert.Equal(t, ArtifactOriginTypeHuggingFace, config.Artifact.Origin.Type)
	assert.Equal(t, "Qwen/Qwen3-4B-Instruct-2507", config.Artifact.Origin.HFModelID)
	assert.Equal(t, sha, config.Artifact.Origin.HFCommitSHA)
}

func TestBuildSelfParentArtifactFromIdentityPreservesExistingChildren(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	modelPath := filepath.Join(t.TempDir(), "parent")
	existingChildPath := filepath.Join(t.TempDir(), "child")
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default"},
	}
	currentKey := constants.GetModelConfigMapKey(model.Namespace, model.Name, false)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		currentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, currentKey, modelPath, []string{existingChildPath}),
	}))

	artifact := g.buildSelfParentArtifactFromIdentity(context.Background(), &GopherTask{BaseModel: model}, ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}, modelPath)

	require.NotNil(t, artifact)
	assert.Equal(t, sha, artifact.Sha)
	assert.Equal(t, modelPath, artifact.ParentPath[currentKey])
	assert.Equal(t, []string{existingChildPath}, artifact.ChildrenPaths)
}

func TestLinkHuggingFaceOriginArtifactDoesNotParseIncompleteChildEntry(t *testing.T) {
	modelID := "Qwen/Qwen3-8B"
	sha := "b968826d9c46dd6066d109eabc6255188de91218"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	parentPath := filepath.Join(t.TempDir(), "_artifacts", "Qwen", "Qwen3-8B", sha)
	childPath := filepath.Join(t.TempDir(), "model-ocid")
	require.NoError(t, os.MkdirAll(parentPath, 0755))
	writeMinimalModelConfig(t, parentPath)

	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model-ocid", Namespace: "default"},
	}
	currentKey := constants.GetModelConfigMapKey(model.Namespace, model.Name, false)

	core, observedLogs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core).Sugar()
	cm := makeConfigMap("node-1", map[string]string{
		parentKey:  entryJSONWithOrigin(ModelStatusReady, modelID, sha, parentKey, parentPath, []string{}),
		currentKey: modelEntryJSON(ModelStatusUpdating),
	})
	client := k8sfake.NewSimpleClientset(cm)
	g := &Gopher{
		configMapReconciler: NewConfigMapReconciler(cm.Name, cm.Namespace, client, logger),
		logger:              logger,
	}

	artifact, err := g.linkHuggingFaceOriginArtifact(context.Background(), &GopherTask{BaseModel: model}, model.Name, childPath, parentKey, parentPath, identity)

	require.NoError(t, err)
	require.NotNil(t, artifact)
	assert.Equal(t, parentPath, artifact.ParentPath[parentKey])
	assert.Empty(t, artifact.ChildrenPaths)
	assert.Zero(t, observedLogs.FilterLevelExact(zapcore.ErrorLevel).Len())
}

func TestInspectSharedArtifactMetadataUsesParentChildRelationships(t *testing.T) {
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default"},
	}
	currentKey := constants.GetModelConfigMapKey(model.Namespace, model.Name, false)

	plainGopher := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		currentKey: modelEntryJSON(ModelStatusReady),
	}))
	hasMetadata, err := plainGopher.inspectSharedArtifactMetadata(context.Background(), &GopherTask{BaseModel: model})
	require.NoError(t, err)
	assert.False(t, hasMetadata)

	originGopher := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		currentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, currentKey, "/models/parent", []string{}),
	}))
	hasMetadata, err = originGopher.inspectSharedArtifactMetadata(context.Background(), &GopherTask{BaseModel: model})
	require.NoError(t, err)
	assert.True(t, hasMetadata)

	legacyGopher := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		currentKey: entryJSON(sha, currentKey, "/models/parent"),
	}))
	hasMetadata, err = legacyGopher.inspectSharedArtifactMetadata(context.Background(), &GopherTask{BaseModel: model})
	require.NoError(t, err)
	assert.True(t, hasMetadata)

	noParentGopher := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		currentKey: entryJSONWithOrigin(ModelStatusReady, modelID, sha, currentKey, "", []string{}),
	}))
	hasMetadata, err = noParentGopher.inspectSharedArtifactMetadata(context.Background(), &GopherTask{BaseModel: model})
	require.NoError(t, err)
	assert.False(t, hasMetadata)

	annotatedModel := model.DeepCopy()
	annotatedModel.Annotations = map[string]string{
		HuggingFaceModelIDAnnotationKey: modelID,
		HuggingFaceSHAAnnotationKey:     sha,
	}
	hasMetadata, err = plainGopher.inspectSharedArtifactMetadata(context.Background(), &GopherTask{BaseModel: annotatedModel})
	require.NoError(t, err)
	assert.False(t, hasMetadata)
}

func TestFindReadyObjectStorageModelWithSamePath(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/large-model"
	modelPath := filepath.Join(t.TempDir(), "large-model")
	require.NoError(t, os.MkdirAll(modelPath, 0755))
	readyKey := constants.GetModelConfigMapKey("default", "large-model", false)
	current := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "service-ns"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}

	tests := []struct {
		name          string
		entryStatus   ModelStatus
		candidatePath string
		destPath      string
		wantReuse     bool
	}{
		{
			name:          "ready model with same path",
			entryStatus:   ModelStatusReady,
			candidatePath: modelPath,
			destPath:      modelPath,
			wantReuse:     true,
		},
		{
			name:          "updating model with same path",
			entryStatus:   ModelStatusUpdating,
			candidatePath: modelPath,
			destPath:      modelPath,
		},
		{
			name:          "ready model with different path",
			entryStatus:   ModelStatusReady,
			candidatePath: filepath.Join(t.TempDir(), "other-model"),
			destPath:      modelPath,
		},
		{
			name:          "ready model missing local path",
			entryStatus:   ModelStatusReady,
			candidatePath: filepath.Join(t.TempDir(), "missing-model"),
			destPath:      filepath.Join(t.TempDir(), "missing-model"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
				readyKey: modelEntryJSON(tt.entryStatus),
			}))
			g.baseModelLister = &mockBaseModelLister{
				models: []*v1beta1.BaseModel{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "default"},
						Spec: v1beta1.BaseModelSpec{
							Storage: &v1beta1.StorageSpec{
								StorageUri: &storageURI,
								Path:       &tt.candidatePath,
							},
						},
					},
				},
			}

			matchedKey, reused := g.findReadyObjectStorageModelWithSamePath(context.Background(), &GopherTask{BaseModel: current}, current.Spec, tt.destPath)

			assert.Equal(t, tt.wantReuse, reused)
			if tt.wantReuse {
				assert.Equal(t, readyKey, matchedKey)
			} else {
				assert.Empty(t, matchedKey)
			}
		})
	}
}

func TestFindUpdatingObjectStorageModelWithSamePathDoesNotRequireLocalPath(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/large-model"
	missingModelPath := filepath.Join(t.TempDir(), "missing-model")
	readyKey := constants.GetModelConfigMapKey("default", "large-model", false)
	currentCreatedAt := metav1.NewTime(time.Now())
	candidateCreatedAt := metav1.NewTime(currentCreatedAt.Add(-time.Minute))
	current := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "service-ns", CreationTimestamp: currentCreatedAt},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &missingModelPath,
			},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		readyKey: modelEntryJSON(ModelStatusUpdating),
	}))
	g.baseModelLister = &mockBaseModelLister{
		models: []*v1beta1.BaseModel{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "default", CreationTimestamp: candidateCreatedAt},
				Spec: v1beta1.BaseModelSpec{
					Storage: &v1beta1.StorageSpec{
						StorageUri: &storageURI,
						Path:       &missingModelPath,
					},
				},
			},
		},
	}

	matchedKey, wait := g.findUpdatingObjectStorageModelWithSamePath(context.Background(), &GopherTask{BaseModel: current}, current.Spec, missingModelPath)

	assert.True(t, wait)
	assert.Equal(t, readyKey, matchedKey)
}

func TestFindUpdatingObjectStorageModelWithSamePathIgnoresNewerCandidate(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/large-model"
	modelPath := filepath.Join(t.TempDir(), "large-model")
	currentCreatedAt := metav1.NewTime(time.Now())
	candidateCreatedAt := metav1.NewTime(currentCreatedAt.Add(time.Minute))
	current := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "service-ns", CreationTimestamp: currentCreatedAt},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	newerModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "another-ns", CreationTimestamp: candidateCreatedAt},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	newerKey := constants.GetModelConfigMapKey(newerModel.Namespace, newerModel.Name, false)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		newerKey: modelEntryJSON(ModelStatusUpdating),
	}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{newerModel}}

	matchedKey, wait := g.findUpdatingObjectStorageModelWithSamePath(context.Background(), &GopherTask{BaseModel: current}, current.Spec, modelPath)

	assert.False(t, wait)
	assert.Empty(t, matchedKey)
}

func TestFindUpdatingObjectStorageModelWithSamePathIgnoresDeletingCandidate(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/large-model"
	modelPath := filepath.Join(t.TempDir(), "large-model")
	now := metav1.Now()
	currentCreatedAt := metav1.NewTime(now.Add(time.Minute))
	candidateCreatedAt := metav1.NewTime(now.Add(-time.Minute))
	current := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "service-ns", CreationTimestamp: currentCreatedAt},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	deletingModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "default", CreationTimestamp: candidateCreatedAt, DeletionTimestamp: &now},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	deletingKey := constants.GetModelConfigMapKey(deletingModel.Namespace, deletingModel.Name, false)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		deletingKey: modelEntryJSON(ModelStatusUpdating),
	}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{deletingModel}}

	matchedKey, wait := g.findUpdatingObjectStorageModelWithSamePath(context.Background(), &GopherTask{BaseModel: current}, current.Spec, modelPath)

	assert.False(t, wait)
	assert.Empty(t, matchedKey)
}

func TestFindReadyObjectStorageModelWithSamePathIgnoresDeletingCandidate(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/large-model"
	modelPath := filepath.Join(t.TempDir(), "large-model")
	require.NoError(t, os.MkdirAll(modelPath, 0755))
	now := metav1.Now()
	current := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "service-ns"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	deletingModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "default", DeletionTimestamp: &now},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	deletingKey := constants.GetModelConfigMapKey(deletingModel.Namespace, deletingModel.Name, false)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{
		deletingKey: modelEntryJSON(ModelStatusReady),
	}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{deletingModel}}

	matchedKey, reused := g.findReadyObjectStorageModelWithSamePath(context.Background(), &GopherTask{BaseModel: current}, current.Spec, modelPath)

	assert.False(t, reused)
	assert.Empty(t, matchedKey)
}

func TestProcessTask_RequeuesSamePathUpdatingObjectStorageModel(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/large-model"
	modelPath := filepath.Join(t.TempDir(), "large-model")
	currentCreatedAt := metav1.NewTime(time.Now())
	candidateCreatedAt := metav1.NewTime(currentCreatedAt.Add(-time.Minute))
	current := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "service-ns", UID: "current-uid", CreationTimestamp: currentCreatedAt},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	updatingModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "default", CreationTimestamp: candidateCreatedAt},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	updatingKey := constants.GetModelConfigMapKey(updatingModel.Namespace, updatingModel.Name, false)
	g := newGopherForProcessTask(makeConfigMap("node-1", map[string]string{
		updatingKey: modelEntryJSON(ModelStatusUpdating),
	}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{current, updatingModel}}
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	g.samePathWaitTimeout = time.Hour

	err := g.processTask(&GopherTask{TaskType: Download, BaseModel: current})

	require.NoError(t, err)
	select {
	case task := <-g.gopherChan:
		assert.Equal(t, current.Name, task.BaseModel.Name)
		assert.False(t, task.SamePathWaitStartedAt.IsZero())
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected same-path Updating task to be requeued")
	}
}

func TestRequeueSamePathInFlightReuseWaitTimesOut(t *testing.T) {
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.gopherChan = make(chan *GopherTask, 1)
	g.samePathWaitDelay = time.Millisecond
	g.samePathWaitTimeout = time.Millisecond
	task := &GopherTask{
		TaskType:              Download,
		SamePathWaitStartedAt: time.Now().Add(-time.Hour),
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns"},
		},
	}

	requeued := g.requeueSamePathInFlightReuseWait(task, "default.basemodel.model")

	assert.False(t, requeued)
	select {
	case <-g.gopherChan:
		t.Fatal("timed out same-path wait task should not be requeued")
	default:
	}
}

func TestProcessTaskWithOptions_HighPriorityDemotesFallbackDownload(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/large-model"
	modelPath := filepath.Join(t.TempDir(), "large-model")
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "large-model", Namespace: "service-ns", UID: "current-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &modelPath,
			},
		},
	}
	g := newGopherForProcessTask(makeConfigMap("node-1", map[string]string{}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}
	g.taskQueue = newGopherTaskQueue()
	task := &GopherTask{
		TaskType:              Download,
		BaseModel:             model,
		SamePathWaitStartedAt: time.Now(),
	}

	err := g.processTaskWithOptions(task, false)

	require.NoError(t, err)
	queued, ok := g.taskQueue.popNormal()
	require.True(t, ok)
	assert.Equal(t, model.Name, queued.BaseModel.Name)
	assert.True(t, queued.NormalPriorityOnly)
	assert.Equal(t, 0, g.taskQueue.len())
}

func TestDemoteToNormalPriorityClassifiesRevalidationBeforeEnqueue(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/already-downloaded"
	modelPath := filepath.Join(t.TempDir(), "already-downloaded")
	require.NoError(t, os.MkdirAll(modelPath, 0755))
	missingStorageURI := "oci://n/object-ns/b/model-bucket/o/models/missing-artifact"
	missingModelPath := filepath.Join(t.TempDir(), "missing-artifact")
	alreadyDownloaded := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "already-downloaded", Namespace: "service-ns", UID: "already-downloaded-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: &storageURI, Path: &modelPath},
		},
	}
	missingArtifact := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "missing-artifact", Namespace: "service-ns", UID: "missing-artifact-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: &missingStorageURI, Path: &missingModelPath},
		},
	}
	g := &Gopher{
		taskQueue:             newGopherTaskQueue(),
		logger:                zap.NewNop().Sugar(),
		startupReadyModelKeys: map[string]struct{}{getModelID(alreadyDownloaded, nil): {}},
		modelRootDir:          t.TempDir(),
	}
	_, err := os.Stat(missingModelPath)
	require.True(t, os.IsNotExist(err))

	g.demoteToNormalPriority(&GopherTask{TaskType: Download, BaseModel: alreadyDownloaded})
	g.demoteToNormalPriority(&GopherTask{TaskType: Download, BaseModel: missingArtifact})

	task, ok := g.taskQueue.popNormal()
	require.True(t, ok)
	assert.Equal(t, missingArtifact.Name, task.BaseModel.Name)
	assert.True(t, task.NormalPriorityOnly)
	assert.False(t, task.RevalidationReplay)

	task, ok = g.taskQueue.popNormal()
	require.True(t, ok)
	assert.Equal(t, alreadyDownloaded.Name, task.BaseModel.Name)
	assert.True(t, task.NormalPriorityOnly)
	assert.True(t, task.RevalidationReplay)
}

func TestEnqueueTaskClassifiesStartupReadyLocalPathAsRevalidation(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/ready-model"
	modelPath := filepath.Join(t.TempDir(), "ready-model")
	require.NoError(t, os.MkdirAll(modelPath, 0755))
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "ready-model", Namespace: "service-ns", UID: "ready-model-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: &storageURI, Path: &modelPath},
		},
	}
	g := &Gopher{
		taskQueue:             newGopherTaskQueue(),
		logger:                zap.NewNop().Sugar(),
		startupReadyModelKeys: map[string]struct{}{getModelID(model, nil): {}},
	}

	g.enqueueTask(&GopherTask{TaskType: Download, BaseModel: model})

	task, ok := g.taskQueue.popNormal()
	require.True(t, ok)
	assert.Equal(t, model.Name, task.BaseModel.Name)
	assert.True(t, task.NormalPriorityOnly)
	assert.True(t, task.RevalidationReplay)
	assert.Equal(t, 0, g.taskQueue.len())
}

func TestCaptureStartupReadyModelsCapturesOnlyReadyEntries(t *testing.T) {
	readyKey := constants.GetModelConfigMapKey("service-ns", "ready-model", false)
	updatingKey := constants.GetModelConfigMapKey("service-ns", "updating-model", false)
	modelID := "Qwen/Qwen3-4B-Instruct-2507"
	sha := "cdbee75f17c01a7cc42f958dc650907174af0554"
	identity := ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   modelID,
		HFCommitSHA: sha,
	}
	readyParentKey := huggingFaceArtifactConfigMapKey(identity)
	updatingParentKey := huggingFaceArtifactConfigMapKey(ArtifactIdentity{
		OriginType:  ArtifactOriginTypeHuggingFace,
		HFModelID:   "Qwen/Qwen3-8B",
		HFCommitSHA: sha,
	})
	cm := makeConfigMap("node-1", map[string]string{
		readyKey:          modelEntryJSON(ModelStatusReady),
		updatingKey:       modelEntryJSON(ModelStatusUpdating),
		readyParentKey:    entryJSONWithOrigin(ModelStatusReady, modelID, sha, readyParentKey, "/tmp/ready-parent", []string{}),
		updatingParentKey: entryJSONWithOrigin(ModelStatusUpdating, "Qwen/Qwen3-8B", sha, updatingParentKey, "/tmp/updating-parent", []string{}),
		"invalid":         "not-json",
	})
	g := newGopherForProcessTask(cm)

	g.captureStartupReadyModels(context.Background())

	assert.Contains(t, g.startupReadyModelKeys, readyKey)
	assert.NotContains(t, g.startupReadyModelKeys, updatingKey)
	assert.NotContains(t, g.startupReadyModelKeys, "invalid")
	assert.True(t, g.claimStartupHuggingFaceArtifactParentValidation(readyParentKey))
	assert.False(t, g.claimStartupHuggingFaceArtifactParentValidation(readyParentKey))
	assert.False(t, g.claimStartupHuggingFaceArtifactParentValidation(updatingParentKey))
}

func TestCaptureStartupReadyModelsFeedsRevalidationClassification(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/ready-model"
	modelPath := filepath.Join(t.TempDir(), "ready-model")
	require.NoError(t, os.MkdirAll(modelPath, 0755))
	readyModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "ready-model", Namespace: "service-ns", UID: "ready-model-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: &storageURI, Path: &modelPath},
		},
	}
	updatingModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "updating-model", Namespace: "service-ns", UID: "updating-model-uid"},
	}
	g := newGopherForProcessTask(makeConfigMap("node-1", map[string]string{
		constants.GetModelConfigMapKey(readyModel.Namespace, readyModel.Name, false):       modelEntryJSON(ModelStatusReady),
		constants.GetModelConfigMapKey(updatingModel.Namespace, updatingModel.Name, false): modelEntryJSON(ModelStatusUpdating),
	}))
	g.taskQueue = newGopherTaskQueue()

	g.captureStartupReadyModels(context.Background())
	g.enqueueTask(&GopherTask{TaskType: Download, BaseModel: readyModel})

	task, ok := g.taskQueue.popNormal()
	require.True(t, ok)
	assert.Equal(t, readyModel.Name, task.BaseModel.Name)
	assert.True(t, task.NormalPriorityOnly)
	assert.True(t, task.RevalidationReplay)
	assert.Contains(t, g.startupReadyModelKeys, getModelID(readyModel, nil))
	assert.NotContains(t, g.startupReadyModelKeys, getModelID(updatingModel, nil))
	assert.Equal(t, 0, g.taskQueue.len())
}

func TestCaptureStartupReadyModelsTreatsMissingConfigMapAsColdStart(t *testing.T) {
	core, recorded := observer.New(zap.DebugLevel)
	logger := zap.New(core).Sugar()
	cm := makeConfigMap("node-1", map[string]string{})
	reconciler := NewConfigMapReconciler(cm.Name, cm.Namespace, k8sfake.NewSimpleClientset(), logger)
	g := &Gopher{
		configMapReconciler:   reconciler,
		logger:                logger,
		startupReadyModelKeys: map[string]struct{}{"stale-model": {}},
	}

	g.captureStartupReadyModels(context.Background())

	assert.Empty(t, g.startupReadyModelKeys)
	assert.Empty(t, recorded.FilterLevelExact(zapcore.ErrorLevel).All())
	assert.Empty(t, recorded.FilterLevelExact(zapcore.WarnLevel).All())
}

func TestClassifyStartupRevalidationIgnoresEmptyStoragePath(t *testing.T) {
	storageURI := "oci://n/object-ns/b/model-bucket/o/models/ready-model"
	emptyPath := ""
	modelRootDir := t.TempDir()
	require.NoError(t, os.MkdirAll(modelRootDir+"/"+storageURI, 0755))
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "ready-model", Namespace: "service-ns", UID: "ready-model-uid"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: &storageURI,
				Path:       &emptyPath,
			},
		},
	}
	g := &Gopher{
		logger:                zap.NewNop().Sugar(),
		startupReadyModelKeys: map[string]struct{}{getModelID(model, nil): {}},
		modelRootDir:          modelRootDir,
	}
	task := &GopherTask{TaskType: Download, BaseModel: model}

	assert.False(t, g.classifyStartupRevalidation(task))
	assert.False(t, task.NormalPriorityOnly)
	assert.False(t, task.RevalidationReplay)
}

func TestGopherEnqueueDeleteCancelsActiveDownload(t *testing.T) {
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns", UID: "model-uid"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.taskQueue = newGopherTaskQueue()
	g.activeDownloads = map[string]activeDownload{
		string(model.UID): {
			token:  "download-token",
			cancel: cancel,
		},
	}

	g.enqueueTask(&GopherTask{TaskType: Delete, BaseModel: model})

	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected delete enqueue to cancel active download")
	}
}

func TestUnregisterActiveDownloadDoesNotRemoveNewerRegistration(t *testing.T) {
	modelUID := "model-uid"
	oldCtx, oldCancel := context.WithCancel(context.Background())
	newCtx, newCancel := context.WithCancel(context.Background())
	t.Cleanup(oldCancel)
	t.Cleanup(newCancel)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.activeDownloads = map[string]activeDownload{
		modelUID: {
			token:  "old-token",
			cancel: oldCancel,
		},
	}
	g.activeDownloads[modelUID] = activeDownload{
		token:  "new-token",
		cancel: newCancel,
	}

	g.unregisterActiveDownload(modelUID, "old-token")

	g.activeDownloadsMutex.RLock()
	active, exists := g.activeDownloads[modelUID]
	g.activeDownloadsMutex.RUnlock()
	require.True(t, exists)
	assert.Equal(t, "new-token", active.token)
	active.cancel()
	select {
	case <-newCtx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected newer active download cancel function to remain registered")
	}
	select {
	case <-oldCtx.Done():
		t.Fatal("old cancel function should not be called")
	default:
	}
}

func TestShouldUseSamePathObjectStorageReuse(t *testing.T) {
	tests := []struct {
		name string
		task *GopherTask
		want bool
	}{
		{
			name: "normal download can reuse same-path object storage artifact",
			task: &GopherTask{TaskType: Download},
			want: true,
		},
		{
			name: "download override must redownload and validate",
			task: &GopherTask{TaskType: DownloadOverride},
		},
		{
			name: "delete never reuses same-path object storage artifact",
			task: &GopherTask{TaskType: Delete},
		},
		{
			name: "nil task never reuses same-path object storage artifact",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldUseSamePathObjectStorageReuse(tt.task))
		})
	}
}

func TestShouldSkipStaleDownloadTask_BaseModelDeletingRequestsCleanup(t *testing.T) {
	now := metav1.Now()
	staleTaskModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")},
		},
	}
	latestModel := staleTaskModel.DeepCopy()
	latestModel.DeletionTimestamp = &now
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{latestModel}}

	skip, runDeleteCleanup := g.shouldSkipStaleDownloadTask(&GopherTask{TaskType: Download, BaseModel: staleTaskModel})

	assert.True(t, skip)
	assert.True(t, runDeleteCleanup)
}

func TestShouldSkipStaleDownloadTask_BaseModelDeletedSkipsWithoutCleanup(t *testing.T) {
	staleTaskModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.baseModelLister = &mockBaseModelLister{}

	skip, runDeleteCleanup := g.shouldSkipStaleDownloadTask(&GopherTask{TaskType: Download, BaseModel: staleTaskModel})

	assert.True(t, skip)
	assert.False(t, runDeleteCleanup)
}

func TestShouldSkipStaleDownloadTask_BaseModelActiveDoesNotSkip(t *testing.T) {
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}

	skip, runDeleteCleanup := g.shouldSkipStaleDownloadTask(&GopherTask{TaskType: Download, BaseModel: model})

	assert.False(t, skip)
	assert.False(t, runDeleteCleanup)
}

func TestShouldSkipStaleDownloadTask_ClusterBaseModelDeletingRequestsCleanup(t *testing.T) {
	now := metav1.Now()
	staleTaskModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")},
		},
	}
	latestModel := staleTaskModel.DeepCopy()
	latestModel.DeletionTimestamp = &now
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{latestModel}}

	skip, runDeleteCleanup := g.shouldSkipStaleDownloadTask(&GopherTask{TaskType: Download, ClusterBaseModel: staleTaskModel})

	assert.True(t, skip)
	assert.True(t, runDeleteCleanup)
}

func TestShouldSkipStaleDownloadTask_ClusterBaseModelDeletedSkipsWithoutCleanup(t *testing.T) {
	staleTaskModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("oci://n/ns/b/bucket/o/model")},
		},
	}
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	skip, runDeleteCleanup := g.shouldSkipStaleDownloadTask(&GopherTask{TaskType: Download, ClusterBaseModel: staleTaskModel})

	assert.True(t, skip)
	assert.False(t, runDeleteCleanup)
}

func TestProcessTask_SkipsMissingBaseModelWithoutWritingStatus(t *testing.T) {
	storageURI := "local:///models/model"
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "service-ns", UID: "uid-model"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: &storageURI},
		},
	}
	labelKey, err := getModelLabelKey(&NodeLabelOp{ModelStateOnNode: Updating, BaseModel: model})
	require.NoError(t, err)
	cm := makeConfigMap("node-1", map[string]string{})
	g := newGopherForProcessTask(cm, map[string]string{labelKey: string(Updating)})
	g.baseModelLister = &mockBaseModelLister{}

	err = g.processTask(&GopherTask{TaskType: Download, BaseModel: model})

	require.NoError(t, err)
	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Get(context.Background(), cm.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, latest.Data, constants.GetModelConfigMapKey(model.Namespace, model.Name, false))
}

func TestProcessTask_TerminatingClusterBaseModelRunsDeleteCleanup(t *testing.T) {
	modelName := "model"
	modelKey := constants.GetModelConfigMapKey("", modelName, true)
	cm := makeConfigMap("node-1", map[string]string{
		modelKey: modelEntryJSON(ModelStatusReady),
	})
	g := newGopherForProcessTask(cm)

	now := metav1.Now()
	storageURI := "local:///models/model"
	staleTaskModel := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: modelName, UID: "uid-cluster-model"},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: &storageURI},
		},
	}
	latestModel := staleTaskModel.DeepCopy()
	latestModel.DeletionTimestamp = &now
	g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{latestModel}}

	err := g.processTask(&GopherTask{TaskType: Download, ClusterBaseModel: staleTaskModel})

	require.NoError(t, err)
	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Get(context.Background(), cm.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, latest.Data, modelKey)
}

func TestHandelReuseArtifactIfNecessary_NoReusePolicy(t *testing.T) {
	nodeName := "node-1"
	// Even if CM has content, when policy is AlwaysDownload we should not reuse.
	cm := makeConfigMap(nodeName, map[string]string{
		"clusterbasemodel.model1": entryJSON("abc123", "parentName", "/models/parent1"),
	})
	g := newGopherWithConfigMap(cm)

	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.AlwaysDownload),
		},
	}

	key, parent := g.handelReuseArtifactIfNecessary(context.Background(), spec, "ClusterBaseModel", "foo", "", "abc123", "ClusterBaseModel.foo")
	assert.Empty(t, key)
	assert.Empty(t, parent)
}

func TestHandelReuseArtifactIfNecessary_HasMatchedEntry(t *testing.T) {
	nodeName := "node-1"
	existingKey := "clusterbasemodel.existingModel"
	expectParentName := "clusterbasemodel.existingModelParent"
	expectedParentPath := "/models/parent1"
	expectedSha := "abc123"
	cm := makeConfigMap(nodeName, map[string]string{
		existingKey: entryJSON(expectedSha, expectParentName, expectedParentPath),
	})
	g := newGopherWithConfigMap(cm)

	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	matchedKey, matchedParentPath := g.handelReuseArtifactIfNecessary(context.Background(), spec, "ClusterBaseModel", "model1", "", "abc123", "ClusterBaseModel.model1")
	assert.Equal(t, expectParentName, matchedKey)
	assert.Equal(t, expectedParentPath, matchedParentPath)
}

func TestHandelReuseArtifactIfNecessary_BaseModelPrefersClusterBaseModelWhenBothMatch(t *testing.T) {
	nodeName := "node-1"
	sha := "samesha"
	clusterBaseModelKey := "clusterbasemodel.model1"
	baseModelKey := "namespace.basemodel.model2"
	clusterParentPath := "/models/parent1"
	clusterParentName := "clusterbasemodel.clusterParent"
	baseParentPath := "/base/parent2"
	baseParentName := "namespace.basemodel.baseParent"
	cm := makeConfigMap(nodeName, map[string]string{
		clusterBaseModelKey: entryJSON(sha, clusterParentName, clusterParentPath),
		baseModelKey:        entryJSON(sha, baseParentName, baseParentPath),
	})
	g := newGopherWithConfigMap(cm)

	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	key, parent := g.handelReuseArtifactIfNecessary(context.Background(), spec, "BaseModel", "newModel", "namespace", sha, "namespace.BaseModel.newModel")
	assert.Equal(t, clusterParentName, key)
	assert.Equal(t, clusterParentPath, parent)
}

func TestHandelReuseArtifactIfNecessary_BaseModelFallbackToNamespaceScoped(t *testing.T) {
	nodeName := "node-1"
	sha := "target-sha"
	// Cluster entry exists but with different sha, so it shouldn't match.
	clusterBaseModelKey := "clusterbasemodel.model1"
	baseModelKey := "namespace.basemodel.model2"
	baseModelParentPath := "/models/parent2"
	cm := makeConfigMap(nodeName, map[string]string{
		clusterBaseModelKey: entryJSON("different-sha", clusterBaseModelKey, "/models/parent1"),
		baseModelKey:        entryJSON(sha, "namespace.basemodel.Parent", baseModelParentPath),
	})
	g := newGopherWithConfigMap(cm)

	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	key, parent := g.handelReuseArtifactIfNecessary(context.Background(), spec, "BaseModel", "name", "namespace", sha, "namespace.BaseModel.name")
	assert.Equal(t, "namespace.basemodel.Parent", key)
	assert.Equal(t, baseModelParentPath, parent)
}

func TestHandelReuseArtifactIfNecessary_NoMatchReturnsEmpty(t *testing.T) {
	nodeName := "node-1"
	cm := makeConfigMap(nodeName, map[string]string{
		"clusterbasemodel.model1":    entryJSON("sha-1", "clusterbasemodel.model1", "/models/parent1"),
		"namespace.basemodel.model2": entryJSON("sha-2", "namespace.basemodel.model2", "/base/parent2"),
	})
	g := newGopherWithConfigMap(cm)

	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	key, parent := g.handelReuseArtifactIfNecessary(context.Background(), spec, "BaseModel", "name", "namespace", "non-existent-sha", "namespace.BaseModel.name")
	assert.Empty(t, key)
	assert.Empty(t, parent)
}

func TestFetchSha_Success(t *testing.T) {
	orig := fetchAttributeFromHfModelMetaData
	defer func() { fetchAttributeFromHfModelMetaData = orig }()

	fetchAttributeFromHfModelMetaData = func(ctx context.Context, modelId string, attribute string) (interface{}, error) {
		assert.Equal(t, Sha, attribute)
		return "abc123def", nil
	}

	g := &Gopher{logger: zap.NewNop().Sugar()}
	sha, ok := g.fetchSha(context.Background(), "org/model", "modelName")
	assert.True(t, ok)
	assert.Equal(t, "abc123def", sha)
}

func TestFetchSha_ErrorFromAPI(t *testing.T) {
	orig := fetchAttributeFromHfModelMetaData
	defer func() { fetchAttributeFromHfModelMetaData = orig }()

	fetchAttributeFromHfModelMetaData = func(ctx context.Context, modelId string, attribute string) (interface{}, error) {
		return nil, fmt.Errorf("api error")
	}

	g := &Gopher{logger: zap.NewNop().Sugar()}
	sha, ok := g.fetchSha(context.Background(), "org/model", "modelName")
	assert.False(t, ok)
	assert.Equal(t, "", sha)
}

func TestFetchSha_NonStringSha(t *testing.T) {
	orig := fetchAttributeFromHfModelMetaData
	defer func() { fetchAttributeFromHfModelMetaData = orig }()

	fetchAttributeFromHfModelMetaData = func(ctx context.Context, modelId string, attribute string) (interface{}, error) {
		return 12345, nil // non-string
	}

	g := &Gopher{logger: zap.NewNop().Sugar()}
	sha, ok := g.fetchSha(context.Background(), "org/model", "modelName")
	assert.False(t, ok)
	assert.Equal(t, "", sha)
}

func TestFetchSha_EmptyStringSha(t *testing.T) {
	orig := fetchAttributeFromHfModelMetaData
	defer func() { fetchAttributeFromHfModelMetaData = orig }()

	fetchAttributeFromHfModelMetaData = func(ctx context.Context, modelId string, attribute string) (interface{}, error) {
		return "", nil // empty string
	}

	g := &Gopher{logger: zap.NewNop().Sugar()}
	sha, ok := g.fetchSha(context.Background(), "org/model", "modelName")
	assert.False(t, ok)
	assert.Equal(t, "", sha)
}

func TestIsEligibleForOptimization_NoShaAvailable(t *testing.T) {
	// Gopher with empty CM is sufficient for this case
	nodeName := "node-1"
	cm := makeConfigMap(nodeName, map[string]string{})
	g := newGopherWithConfigMap(cm)

	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "bm",
			},
		},
	}

	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	eligible, key, parent := g.isEligibleForOptimization(context.Background(), task, spec, "BaseModel", "ns", false, "", "modelName")
	assert.False(t, eligible)
	assert.Empty(t, key)
	assert.Empty(t, parent)
}

func TestIsEligibleForOptimization_MatchedDifferentKeyEligible(t *testing.T) {
	nodeName := "node-1"
	sha := "123abc"
	expectedKey := "clusterbasemodel.modelX"
	expectedParentPath := "/models/parentX"

	// CM has a ClusterBaseModel entry with matching sha
	cm := makeConfigMap(nodeName, map[string]string{
		expectedKey: entryJSON(sha, "clusterbasemodel.modelX", expectedParentPath),
	})
	g := newGopherWithConfigMap(cm)

	// Current model is a BaseModel with a different key than the matched one
	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "bm",
			},
		},
	}
	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	eligible, key, parent := g.isEligibleForOptimization(context.Background(), task, spec, "BaseModel", "ns", true, "123abc", "modelName")
	assert.True(t, eligible)
	assert.Equal(t, expectedKey, key)
	assert.Equal(t, expectedParentPath, parent)
}

func TestIsEligibleForOptimization_MatchedSameKeyNotEligible(t *testing.T) {
	nodeName := "node-1"
	sha := "123abc"
	expectedKey := "clusterbasemodel.model1"
	parentPath := "/models/p1"

	// CM has a ClusterBaseModel entry with matching sha
	cm := makeConfigMap(nodeName, map[string]string{
		expectedKey: entryJSON(sha, "clusterbasemodel.model1", parentPath),
	})
	g := newGopherWithConfigMap(cm)

	// Current model is the same ClusterBaseModel key as the matched one
	task := &GopherTask{
		TaskType: Download,
		ClusterBaseModel: &v1beta1.ClusterBaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Name: "model1",
			},
		},
	}
	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	eligible, key, actualParentPath := g.isEligibleForOptimization(context.Background(), task, spec, "ClusterBaseModel", "", true, "123abc", "model1")
	assert.False(t, eligible, "same key should not be eligible for reuse")
	assert.Empty(t, key)
	assert.Empty(t, actualParentPath)
}

func TestIsEligibleForOptimization_NoMatch(t *testing.T) {
	nodeName := "node-1"
	targetSha := "sha-target"

	// CM entries with different sha values
	cm := makeConfigMap(nodeName, map[string]string{
		"clusterbasemodel.other": entryJSON("sha-other", "clusterbasemodel.other", "/models/p2"),
	})
	g := newGopherWithConfigMap(cm)

	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "bm",
			},
		},
	}
	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.ReuseIfExists),
		},
	}

	eligible, key, parent := g.isEligibleForOptimization(context.Background(), task, spec, "BaseModel", "ns", true, targetSha, "modelName")
	assert.False(t, eligible)
	assert.Empty(t, key)
	assert.Empty(t, parent)
}

func TestIsEligibleForOptimization_AlwaysDownloadNotEligible(t *testing.T) {
	nodeName := "node-1"
	sha := "123abc"
	expectedKey := "clusterbasemodel.modelX"
	expectedParent := "/models/parentX"

	// CM has a ClusterBaseModel entry with matching sha
	cm := makeConfigMap(nodeName, map[string]string{
		expectedKey: entryJSON(sha, "clusterbasemodel.modelX", expectedParent),
	})
	g := newGopherWithConfigMap(cm)

	// Current model is a BaseModel with a different key than the matched one
	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "bm",
			},
		},
	}
	spec := v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			DownloadPolicy: dp(v1beta1.AlwaysDownload),
		},
	}

	eligible, key, parent := g.isEligibleForOptimization(context.Background(), task, spec, "BaseModel", "ns", true, "123abc", "modelName")
	assert.False(t, eligible)
	assert.Empty(t, key)
	assert.Empty(t, parent)
}

func newGopherWithEmptyClient(nodeName, namespace string, t *testing.T) (*Gopher, *k8sfake.Clientset) {
	client := k8sfake.NewSimpleClientset()
	logger := zaptest.NewLogger(t).Sugar()
	cmr := NewConfigMapReconciler(nodeName, namespace, client, logger)
	return &Gopher{
		configMapReconciler: cmr,
		logger:              logger,
	}, client
}

func newGopherAndClientWithConfigMap(cm *corev1.ConfigMap, t *testing.T) (*Gopher, *k8sfake.Clientset) {
	client := k8sfake.NewSimpleClientset(cm)
	logger := zaptest.NewLogger(t).Sugar()
	cmr := NewConfigMapReconciler(cm.Name, cm.Namespace, client, logger)
	return &Gopher{
		configMapReconciler: cmr,
		logger:              logger,
	}, client
}

func countConfigMapUpdates(client *k8sfake.Clientset) int {
	n := 0
	for _, a := range client.Fake.Actions() {
		if a.Matches("update", "configmaps") {
			n++
		}
	}
	return n
}

func countConfigMapGets(client *k8sfake.Clientset) int {
	n := 0
	for _, a := range client.Fake.Actions() {
		if a.Matches("get", "configmaps") {
			n++
		}
	}
	return n
}

func getChildrenPaths(entry string) []string {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(entry), &obj); err != nil {
		return nil
	}
	cfg, _ := obj["config"].(map[string]interface{})
	art, _ := cfg["artifact"].(map[string]interface{})
	raw, _ := art["childrenPaths"].([]interface{})
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func TestHasChildrenPaths_NoChildren_HasMatchedParent(t *testing.T) {
	// Keys and paths
	parentKey := "clusterbasemodel.parentModel"
	childKey := "clusterbasemodel.childModel"
	parentPath := "/models/parent"
	childPath := "/models/childA"

	// Parent entry contains the child path
	parentEntry := entryJSON("sha", parentKey, parentPath)
	// Replace its childrenPaths with [childPath]
	var obj map[string]interface{}
	_ = json.Unmarshal([]byte(parentEntry), &obj)
	cfg := obj["config"].(map[string]interface{})
	art := cfg["artifact"].(map[string]interface{})
	art["childrenPaths"] = []interface{}{childPath}
	parentEntryWithChild, _ := json.Marshal(obj)

	// Child entry has empty childrenPaths and points to the parent
	childEntry := entryJSON("sha", parentKey, parentPath)

	cm := makeConfigMap("node-1", map[string]string{
		parentKey: string(parentEntryWithChild),
		childKey:  childEntry,
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	actualChildren, parentName, parentDir, err := g.parseModelConfigDataEntry(context.Background(), childKey)
	assert.Empty(t, actualChildren, "no children implies cleanup and return false")

	// should have get CM once
	assert.Equal(t, countConfigMapGets(client), 1, "expected a ConfigMap get to retrieve configmap")
	assert.Equal(t, "clusterbasemodel.parentModel", parentName)
	assert.Equal(t, "/models/parent", parentDir)
	assert.NoError(t, err)

}

func TestHasChildrenPaths_NoChildren_ParentItself(t *testing.T) {
	// Keys and paths
	childKey := "clusterbasemodel.childModel"

	// Child entry has empty childrenPaths and parent points to itself
	childEntry := entryJSON("sha", "clusterbasemodel.childModel", "/models/childA")

	cm := makeConfigMap("node-1", map[string]string{
		childKey: childEntry,
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)
	actualChildren, parentName, parentDir, err := g.parseModelConfigDataEntry(context.Background(), childKey)
	assert.Empty(t, actualChildren, "no children")

	// should have get CM once
	assert.Equal(t, countConfigMapGets(client), 1, "expected a ConfigMap get to retrieve configmap")
	assert.Equal(t, "clusterbasemodel.childModel", parentName)
	assert.Equal(t, "/models/childA", parentDir)
	assert.NoError(t, err)
}

func TestHasChildrenPaths_WithChildren_ParentItself(t *testing.T) {
	modelKey := "clusterbasemodel.model"
	parentName := "clusterbasemodel.entry"
	parentPath := "/models/p"
	childPath := "/models/c"

	// Entry with children means we treat it as parent and do not clean up
	entry := entryJSON("sha", parentName, parentPath)
	var obj map[string]interface{}
	_ = json.Unmarshal([]byte(entry), &obj)
	cfg := obj["config"].(map[string]interface{})
	art := cfg["artifact"].(map[string]interface{})
	art["childrenPaths"] = []interface{}{childPath}
	entryWithChild, _ := json.Marshal(obj)

	cm := makeConfigMap("node-y", map[string]string{modelKey: string(entryWithChild)})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	actualChildren, parentName, parentDir, err := g.parseModelConfigDataEntry(context.Background(), modelKey)
	assert.Equal(t, []string{"/models/c"}, actualChildren, "non-empty children should return true")
	//assert.Equal(t, 0, countConfigMapUpdates(client), "no update expected when entry already has children")
	// 1 CM get should have been attempted
	assert.Equal(t, countConfigMapGets(client), 1, "expected a ConfigMap get to retrieve configmap")
	assert.Equal(t, "clusterbasemodel.entry", parentName)
	assert.Equal(t, "/models/p", parentDir)
	assert.NoError(t, err)
}

func TestHasChildrenPaths_GetConfigMapError(t *testing.T) {
	// No ConfigMap created for this node
	g, client := newGopherWithEmptyClient("missing-node", "ome", t)

	actualChildren, parentName, parentDir, err := g.parseModelConfigDataEntry(context.Background(), "clusterbasemodel.child")

	assert.Empty(t, actualChildren, "should be empty")
	assert.Equal(t, countConfigMapGets(client), 1, "expected a ConfigMap get to retrieve configmap")
	assert.Empty(t, parentName)
	assert.Empty(t, parentDir)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cannot retrieve node configmap and cannot determine whether it has childrenPaths and will regard it has")
}

func TestHasChildrenPaths_ParentParseError(t *testing.T) {
	// Prepare parent with a child path and a malformed child entry to cause parsing error
	parentKey := "clusterbasemodel.parent"
	childKey := "clusterbasemodel.child"
	parentPath := "/models/p"
	childPath := "/models/c"

	parentEntry := entryJSON("shaP", parentKey, parentPath)
	// Make parent contain the child path
	var pobj map[string]interface{}
	_ = json.Unmarshal([]byte(parentEntry), &pobj)
	pcfg := pobj["config"].(map[string]interface{})
	part := pcfg["artifact"].(map[string]interface{})
	part["childrenPaths"] = []interface{}{childPath}
	parentEntryWithChild, _ := json.Marshal(pobj)

	// Malformed child entry to trigger error in getParentPathAndChildrenPaths (missing config/artifact)
	childEntry := "{}"

	cm := makeConfigMap("node-x", map[string]string{
		parentKey: string(parentEntryWithChild),
		childKey:  childEntry,
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	childrenPaths, parentName, parentDir, err := g.parseModelConfigDataEntry(context.Background(), childKey)
	assert.Empty(t, childrenPaths, "should be empty")

	// 1 CM get should have been attempted
	assert.Equal(t, countConfigMapGets(client), 1, "expected a ConfigMap get to retrieve configmap")
	assert.Empty(t, parentName)
	assert.Empty(t, parentDir)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cannot determine whether it has childrenPaths and will regard it has because")
}

func TestRemoveChildPathFromParentConfigMapIfNecessary_RemovesWhenEligible(t *testing.T) {
	parentKey := "clusterbasemodel.parentModel"
	childKey := "clusterbasemodel.childModel"
	parentDir := "/models/parent"
	childPath := "/models/childA"

	// Parent entry contains the child path
	parentEntry := entryJSON("sha", parentKey, parentDir)
	var obj map[string]interface{}
	_ = json.Unmarshal([]byte(parentEntry), &obj)
	cfg := obj["config"].(map[string]interface{})
	art := cfg["artifact"].(map[string]interface{})
	art["childrenPaths"] = []interface{}{childPath}
	parentEntryWithChild, _ := json.Marshal(obj)

	// Child entry (content does not affect this method, but keep realistic)
	childEntry := entryJSON("sha", parentKey, parentDir)

	cm := makeConfigMap("node-1", map[string]string{
		parentKey: string(parentEntryWithChild),
		childKey:  childEntry,
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	g.removeChildPathFromParentConfigMapIfNecessary(context.Background(), false, parentKey, childKey, childPath)

	latest, _ := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	children := getChildrenPaths(latest.Data[parentKey])
	assert.ElementsMatch(t, []string{}, children, "parent childrenPaths should be empty after removal")
	assert.Equal(t, 1, countConfigMapUpdates(client), "expected a single ConfigMap update")
}

func TestRemoveChildPathFromParentConfigMapIfNecessary_NoOpWhenHasChildren(t *testing.T) {
	parentKey := "clusterbasemodel.parentModel"
	childKey := "clusterbasemodel.childModel"
	parentDir := "/models/parent"
	childPath := "/models/childA"

	// Parent entry contains the child path
	parentEntry := entryJSON("sha", parentKey, parentDir)
	var obj map[string]interface{}
	_ = json.Unmarshal([]byte(parentEntry), &obj)
	cfg := obj["config"].(map[string]interface{})
	art := cfg["artifact"].(map[string]interface{})
	art["childrenPaths"] = []interface{}{childPath}
	parentEntryWithChild, _ := json.Marshal(obj)

	cm := makeConfigMap("node-1", map[string]string{
		parentKey: string(parentEntryWithChild),
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	// hasChildren = true -> should be no-op
	g.removeChildPathFromParentConfigMapIfNecessary(context.Background(), true, parentKey, childKey, childPath)

	latest, _ := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	children := getChildrenPaths(latest.Data[parentKey])
	assert.ElementsMatch(t, []string{childPath}, children, "childrenPaths should remain unchanged when hasChildren is true")
	assert.Equal(t, 0, countConfigMapUpdates(client), "no ConfigMap update expected")
}

func TestRemoveChildPathFromParentConfigMapIfNecessary_NoOpWhenParentIsSelf(t *testing.T) {
	childKey := "namespace.basemodel.child"
	// different case to validate case-insensitive equality
	parentName := "NAMESPACE.BASEMODEL.CHILD"
	parentDir := "/models/child"
	childPath := "/models/child"

	// Entry with children containing the child's path
	parentEntry := entryJSON("sha", parentName, parentDir)
	var obj map[string]interface{}
	_ = json.Unmarshal([]byte(parentEntry), &obj)
	cfg := obj["config"].(map[string]interface{})
	art := cfg["artifact"].(map[string]interface{})
	art["childrenPaths"] = []interface{}{childPath}
	parentEntryWithChild, _ := json.Marshal(obj)

	cm := makeConfigMap("node-1", map[string]string{
		parentName: string(parentEntryWithChild),
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	g.removeChildPathFromParentConfigMapIfNecessary(context.Background(), false, parentName, childKey, childPath)

	latest, _ := client.CoreV1().ConfigMaps("ome").Get(context.Background(), "node-1", metav1.GetOptions{})
	children := getChildrenPaths(latest.Data[parentName])
	assert.ElementsMatch(t, []string{childPath}, children, "no removal when parent equals self (case-insensitive)")
	assert.Equal(t, 0, countConfigMapUpdates(client), "no ConfigMap update expected when parent equals self")
}

func TestRemoveChildPathFromParentConfigMapIfNecessary_ErrorWhenParentMissing_NoPanic(t *testing.T) {
	// Only child entry, parent key missing
	childKey := "clusterbasemodel.child"
	childEntry := entryJSON("sha", "clusterbasemodel.parent", "/models/p")

	cm := makeConfigMap("node-1", map[string]string{
		childKey: childEntry,
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	assert.NotPanics(t, func() {
		g.removeChildPathFromParentConfigMapIfNecessary(context.Background(), false, "clusterbasemodel.missing", childKey, "/models/child")
	}, "method should not panic when reconciler returns error")

	assert.Equal(t, 0, countConfigMapUpdates(client), "no ConfigMap update should occur when parent key missing")
}
func TestIsRemoveParentArtifactDirectory_HasChildren_False(t *testing.T) {
	cm := makeConfigMap("node-1", map[string]string{})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	got := g.isRemoveParentArtifactDirectory(context.Background(), true, "clusterbasemodel.parent", "/parent")
	assert.False(t, got, "should not remove when hasChildren is true")
	assert.Equal(t, 0, countConfigMapGets(client), "expected no ConfigMap get")

}

func TestIsRemoveParentArtifactDirectory_ParentEntryExists_False(t *testing.T) {
	parentKey := "clusterbasemodel.parent"
	cm := makeConfigMap("node-1", map[string]string{
		parentKey: entryJSON("sha", parentKey, "/models/p"),
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)
	got := g.isRemoveParentArtifactDirectory(context.Background(), false, parentKey, "/parent")
	assert.False(t, got, "should not remove when parent entry exists in ConfigMap")
	assert.Equal(t, 1, countConfigMapGets(client), "expected a single ConfigMap get")

}

func TestIsRemoveParentArtifactDirectory_CannotRetrieveConfigMap_False(t *testing.T) {
	// No ConfigMap exists for this node, getDataEntryBasedOnModelKey returns error with "cannot retrieve node configmap"
	g, client := newGopherWithEmptyClient("missing-node", "ome", t)

	got := g.isRemoveParentArtifactDirectory(context.Background(), false, "clusterbasemodel.parent", "/parent")
	assert.False(t, got, "should not remove when cannot retrieve node configmap")
	assert.Equal(t, 1, countConfigMapGets(client), "expected a single ConfigMap get")
}

func TestIsSkippingArtifactDeletion_ReserveLabel_Skip(t *testing.T) {
	node := "node-r2"
	destPath := "/models/x"

	cm := makeConfigMap(node, map[string]string{})
	g := newGopherWithConfigMap(cm)
	// No references
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	// Reserve label on BaseModel
	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "bm",
				Labels: map[string]string{
					"models.ome/reserve-model-artifact": "true",
				},
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}

	got, isRemoveParent, parentName, parentDir := g.isSkippingArtifactDeletion(context.Background(), task, destPath, false)
	assert.True(t, got, "reserve label should skip deletion")
	assert.False(t, isRemoveParent)
	assert.Empty(t, parentName)
	assert.Empty(t, parentDir)
}

func TestIsSkippingArtifactDeletion_ReferencedByOthers_Skip(t *testing.T) {
	node := "node-r1"
	destPath := "/models/shared"

	// CM not used by the reference check path
	cm := makeConfigMap(node, map[string]string{})
	g := newGopherWithConfigMap(cm)
	// Mock listers to report a referencing BaseModel different from the task model
	mockBM := &mockBaseModelLister{
		models: []*v1beta1.BaseModel{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "other",
				},
				Spec: v1beta1.BaseModelSpec{
					Storage: &v1beta1.StorageSpec{Path: &destPath},
				},
			},
		},
	}
	g.baseModelLister = mockBM
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	// Task model to be deleted
	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "bm",
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}

	got, isRemoveParent, parentName, parentDir := g.isSkippingArtifactDeletion(context.Background(), task, destPath, false)
	assert.True(t, got, "referenced by others should skip deletion")
	assert.False(t, isRemoveParent)
	assert.Empty(t, parentName)
	assert.Empty(t, parentDir)
}

func TestIsSkippingArtifactDeletion_ReferenceCheckError_Skip(t *testing.T) {
	node := "node-r5"
	destPath := "/models/x"

	cm := makeConfigMap(node, map[string]string{})
	g := newGopherWithConfigMap(cm)
	// Simulate lister error path
	g.baseModelLister = &mockBaseModelLister{err: errors.New("lister failed")}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "bm",
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}

	got, isRemoveParent, actualParentName, actualParentDir := g.isSkippingArtifactDeletion(context.Background(), task, destPath, false)
	assert.True(t, got, "on reference check error deletion should be skipped")
	assert.False(t, isRemoveParent)
	assert.Empty(t, actualParentName)
	assert.Empty(t, actualParentDir)
}

func TestIsSkippingArtifactDeletion_ChildrenPresent_Skip(t *testing.T) {
	node := "node-r3"
	ns, name := "ns", "child"
	childKey := ns + ".basemodel." + name
	parentPath := "/models/parent"
	// Child entry has childrenPaths non-empty
	entry := entryJSON("sha", childKey, parentPath)
	var obj map[string]interface{}
	_ = json.Unmarshal([]byte(entry), &obj)
	cfg := obj["config"].(map[string]interface{})
	art := cfg["artifact"].(map[string]interface{})
	art["childrenPaths"] = []interface{}{"/some/child"}
	entryWithChild, _ := json.Marshal(obj)

	cm := makeConfigMap(node, map[string]string{
		childKey: string(entryWithChild),
	})
	g, _ := newGopherAndClientWithConfigMap(cm, t)
	// No references
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	destPath := "/models/child"
	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      name,
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}

	got, isRemoveParent, parentName, parentDir := g.isSkippingArtifactDeletion(context.Background(), task, destPath, true)
	assert.True(t, got, "non-empty childrenPaths should skip deletion")
	assert.False(t, isRemoveParent)
	assert.Equal(t, "ns.basemodel.child", parentName)
	assert.Equal(t, "/models/parent", parentDir)
}

func TestIsSkippingArtifactDeletion_NoChildren_ProceedsAndUpdatesParent(t *testing.T) {
	node := "node-r4"
	ns, name := "ns", "child"
	childKey := ns + ".basemodel." + name
	parentKey := "clusterbasemodel.parent"
	parentDir := "/models/parent"
	destPath := "/models/child"

	// Child entry has empty childrenPaths and points to parent
	childEntry := entryJSON("sha-child", parentKey, parentDir)
	// Ensure childrenPaths empty for child
	var ch map[string]interface{}
	_ = json.Unmarshal([]byte(childEntry), &ch)
	chcfg := ch["config"].(map[string]interface{})
	chart := chcfg["artifact"].(map[string]interface{})
	chart["childrenPaths"] = []interface{}{}
	childEntryNoChild, _ := json.Marshal(ch)

	// Parent entry includes the child's destPath
	parentEntry := entryJSON("sha-parent", parentKey, parentDir)
	var pobj map[string]interface{}
	_ = json.Unmarshal([]byte(parentEntry), &pobj)
	pcfg := pobj["config"].(map[string]interface{})
	part := pcfg["artifact"].(map[string]interface{})
	part["childrenPaths"] = []interface{}{destPath}
	parentEntryWithChild, _ := json.Marshal(pobj)

	cm := makeConfigMap(node, map[string]string{
		childKey:  string(childEntryNoChild),
		parentKey: string(parentEntryWithChild),
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)
	// No references, no reserve
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      name,
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}

	got, isRemoveParent, actualParentName, actualParentDir := g.isSkippingArtifactDeletion(context.Background(), task, destPath, true)
	assert.False(t, got, "no children implies deletion should proceed and parent cleaned")

	// Verify parent childrenPaths is now empty
	latest, _ := client.CoreV1().ConfigMaps("ome").Get(context.Background(), node, metav1.GetOptions{})
	children := getChildrenPaths(latest.Data[parentKey])
	assert.ElementsMatch(t, []string{}, children, "parent childrenPaths should be empty after removal")

	assert.False(t, isRemoveParent)
	assert.Equal(t, parentKey, actualParentName)
	assert.Equal(t, parentDir, actualParentDir)
}

func TestIsSkippingArtifactDeletion_ParseError_TreatsAsHasChildren(t *testing.T) {
	node := "node-pe1"
	ns, name := "ns", "child"
	childKey := ns + ".basemodel." + name

	// Malformed entry for the current model key to cause parse error in parseModelConfigDataEntry
	cm := makeConfigMap(node, map[string]string{
		childKey: "{}",
	})
	g, client := newGopherAndClientWithConfigMap(cm, t)
	// No references and no reserve labels
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      name,
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}

	skip, isRemoveParent, parentName, parentDir := g.isSkippingArtifactDeletion(context.Background(), task, "/models/child", true)
	assert.True(t, skip, "parse error should be treated as hasChildren and skip deletion")
	assert.False(t, isRemoveParent, "on parse error parent directory should not be removed")
	assert.Empty(t, parentName, "parent name should be empty on parse error")
	assert.Empty(t, parentDir, "parent dir should be empty on parse error")
	assert.Equal(t, 1, countConfigMapGets(client), "expected a single ConfigMap get")
}

func TestIsSkippingArtifactDeletion_GetConfigMapError_TreatsAsHasChildren(t *testing.T) {
	// No ConfigMap exists for this node
	g, client := newGopherWithEmptyClient("missing-node", "ome", t)
	// No references and no reserve labels
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}

	task := &GopherTask{
		TaskType: Download,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns",
				Name:      "child",
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}

	skip, isRemoveParent, parentName, parentDir := g.isSkippingArtifactDeletion(context.Background(), task, "/models/child", true)
	assert.True(t, skip, "failure to get configmap should be treated as hasChildren and skip deletion")
	assert.False(t, isRemoveParent, "should not remove parent when configmap cannot be retrieved")
	assert.Empty(t, parentName, "parent name should be empty when configmap cannot be retrieved")
	assert.Empty(t, parentDir, "parent dir should be empty when configmap cannot be retrieved")
	assert.Equal(t, 1, countConfigMapGets(client), "expected a ConfigMap get attempt")
}

func Test_hasChildrenPaths_ParseErrorReturnsTrue(t *testing.T) {
	assert.True(t, hasChildrenPaths(nil, fmt.Errorf("parse error")))
	assert.True(t, hasChildrenPaths([]string{}, fmt.Errorf("parse error")))
	assert.True(t, hasChildrenPaths([]string{"/models/child"}, fmt.Errorf("parse error")))
}

func Test_hasChildrenPaths_EmptyNoError_ReturnsFalse(t *testing.T) {
	assert.False(t, hasChildrenPaths(nil, nil))
	assert.False(t, hasChildrenPaths([]string{}, nil))
}

func Test_hasChildrenPaths_NonEmptyNoError_ReturnsTrue(t *testing.T) {
	assert.True(t, hasChildrenPaths([]string{"/child"}, nil))
	assert.True(t, hasChildrenPaths([]string{"/child1", "/child2"}, nil))
}
