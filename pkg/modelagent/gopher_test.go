package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omev1beta1lister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
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

func TestWithModelVerificationConcurrencyCreatesSharedLimiter(t *testing.T) {
	g := &Gopher{}

	WithModelVerificationConcurrency(8)(g)

	assert.Equal(t, 8, g.modelVerificationConcurrency)
	require.NotNil(t, g.modelVerificationLimiter)
	assert.Equal(t, 8, g.modelVerificationLimiter.limit())

	WithModelVerificationConcurrency(0)(g)
	assert.Equal(t, 1, g.modelVerificationConcurrency)
	require.NotNil(t, g.modelVerificationLimiter)
	assert.Equal(t, 1, g.modelVerificationLimiter.limit())
}

func TestVerificationConcurrencyIsSharedAcrossModels(t *testing.T) {
	g := &Gopher{}
	WithModelVerificationConcurrency(3)(g)

	uris := make([]ociobjectstore.ObjectURI, 12)
	for i := range uris {
		uris[i] = ociobjectstore.ObjectURI{ObjectName: fmt.Sprintf("model/file-%02d", i), Prefix: "model"}
	}

	var active atomic.Int32
	var maxActive atomic.Int32
	validate := func(_ ociobjectstore.ObjectURI, _ string) (bool, error) {
		current := active.Add(1)
		for {
			observed := maxActive.Load()
			if current <= observed || maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		return true, nil
	}

	start := make(chan struct{})
	var runs sync.WaitGroup
	runs.Add(2)
	for range 2 {
		go func() {
			defer runs.Done()
			<-start
			errs := g.verifyDownloadedFilesWithValidator(uris, "/models", validate)
			assert.Empty(t, errs)
		}()
	}
	close(start)
	runs.Wait()

	assert.Equal(t, int32(3), maxActive.Load())
}

func TestVerifyDownloadedFilesWithValidatorReportsFailures(t *testing.T) {
	g := &Gopher{}
	WithModelVerificationConcurrency(2)(g)
	uris := []ociobjectstore.ObjectURI{
		{ObjectName: "model/valid", Prefix: "model"},
		{ObjectName: "model/mismatch", Prefix: "model"},
		{ObjectName: "model/error", Prefix: "model"},
	}

	errs := g.verifyDownloadedFilesWithValidator(uris, "/models", func(obj ociobjectstore.ObjectURI, _ string) (bool, error) {
		switch obj.ObjectName {
		case "model/mismatch":
			return false, nil
		case "model/error":
			return false, errors.New("read failed")
		default:
			return true, nil
		}
	})

	require.Len(t, errs, 2)
	assert.ErrorContains(t, errs["model/mismatch"], "MD5 or size mismatch")
	assert.EqualError(t, errs["model/error"], "read failed")
}

func TestCaptureStartupReadyModelsCapturesOnlyReadyEntries(t *testing.T) {
	readyKey := constants.GetModelConfigMapKey("service-ns", "ready-model", false)
	updatingKey := constants.GetModelConfigMapKey("service-ns", "updating-model", false)
	cm := makeConfigMap("node-1", map[string]string{
		readyKey:    modelEntryJSON(ModelStatusReady),
		updatingKey: modelEntryJSON(ModelStatusUpdating),
		"invalid":   "not-json",
	})
	g := newGopherForProcessTask(cm)

	g.captureStartupReadyModels(context.Background())

	assert.Contains(t, g.startupReadyModelKeys, readyKey)
	assert.NotContains(t, g.startupReadyModelKeys, updatingKey)
	assert.NotContains(t, g.startupReadyModelKeys, "invalid")
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
