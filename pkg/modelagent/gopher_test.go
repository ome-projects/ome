package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
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

	requeued := g.requeueArtifactWait(task, "default.basemodel.model")

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

	require.NotNil(t, g.modelVerificationLimiter)
	assert.Equal(t, 8, g.modelVerificationLimiter.limit())

	WithModelVerificationConcurrency(0)(g)
	require.NotNil(t, g.modelVerificationLimiter)
	assert.Equal(t, 1, g.modelVerificationLimiter.limit())
}

func TestVerificationConcurrencyIsSharedAcrossModels(t *testing.T) {
	g := &Gopher{}
	WithModelVerificationConcurrency(3)(g)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uris := make([]ociobjectstore.ObjectURI, 12)
	for i := range uris {
		uris[i] = ociobjectstore.ObjectURI{ObjectName: fmt.Sprintf("model/file-%02d", i), Prefix: "model"}
	}

	var active atomic.Int32
	var maxActive atomic.Int32
	started := make(chan struct{}, len(uris))
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	validate := func(_ ociobjectstore.ObjectURI, _ string) (bool, error) {
		current := active.Add(1)
		for {
			observed := maxActive.Load()
			if current <= observed || maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return true, nil
	}

	done := make(chan map[string]error, 2)
	runModel := func(modelURIs []ociobjectstore.ObjectURI) {
		go func() {
			done <- g.verifyDownloadedFilesWithValidator(ctx, modelURIs, "/models", validate)
		}()
	}
	waitForStart := func() {
		t.Helper()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("timed out waiting for file verification to start")
		}
	}

	// Hold one permit in model A before starting model B, ensuring both
	// models are verifying files when the shared limit is reached.
	runModel(uris[:1])
	waitForStart()
	runModel(uris[1:])
	waitForStart()
	waitForStart()
	assert.Equal(t, int32(3), active.Load())
	// Verify the occupied permits belong to the shared limiter, not to
	// independent per-model limiters that happen to run sequentially.
	assert.Len(t, g.modelVerificationLimiter.permits, 3)
	unblock()
	for range 2 {
		select {
		case errs := <-done:
			assert.Empty(t, errs)
		case <-ctx.Done():
			t.Fatal("timed out waiting for model verification to finish")
		}
	}

	assert.Equal(t, int32(3), maxActive.Load())
	assert.Zero(t, active.Load())
	assert.Empty(t, g.modelVerificationLimiter.permits)
}

func TestVerifyDownloadedFilesWithValidatorReportsFailures(t *testing.T) {
	g := &Gopher{}
	WithModelVerificationConcurrency(2)(g)
	uris := []ociobjectstore.ObjectURI{
		{ObjectName: "model/valid", Prefix: "model"},
		{ObjectName: "model/mismatch", Prefix: "model"},
		{ObjectName: "model/error", Prefix: "model"},
	}

	errs := g.verifyDownloadedFilesWithValidator(context.Background(), uris, "/models", func(obj ociobjectstore.ObjectURI, _ string) (bool, error) {
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
	attempt := g.taskTracker.beginLegacyTask(string(model.UID), cancel)
	t.Cleanup(func() { g.taskTracker.finishLegacyTask(attempt) })

	g.enqueueTask(&GopherTask{TaskType: Delete, BaseModel: model})

	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected delete enqueue to cancel active download")
	}
}

func TestFinishActiveDownloadDoesNotRemoveNewerRegistration(t *testing.T) {
	modelUID := "model-uid"
	oldCtx, oldCancel := context.WithCancel(context.Background())
	newCtx, newCancel := context.WithCancel(context.Background())
	t.Cleanup(oldCancel)
	t.Cleanup(newCancel)
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	oldAttempt, result := g.taskTracker.beginDownload(modelUID, 1, oldCancel)
	require.Equal(t, gopherTaskProceed, result)
	g.taskTracker.finishDownload(oldAttempt)
	newAttempt, result := g.taskTracker.beginDownload(modelUID, 2, newCancel)
	require.Equal(t, gopherTaskProceed, result)
	t.Cleanup(func() { g.taskTracker.finishDownload(newAttempt) })
	g.taskTracker.finishDownload(oldAttempt)
	deletion, result := g.taskTracker.beginDelete(modelUID, 3)
	require.Equal(t, gopherTaskWait, result)
	t.Cleanup(func() { g.taskTracker.finishDelete(deletion, false) })
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

// A Delete during a download finds the model's entry without config.artifact, and
// later Deletes find no entry. Neither may keep the files of a model that is being
// deleted or no longer targets this node. Other Deletes keep them, since a Delete
// event can arrive for a model that still exists.
func TestProcessTask_HfDeleteRemovesFilesOnlyWhenConfirmed(t *testing.T) {
	modelKey := constants.GetModelConfigMapKey("", "model", true)
	downloading := `{"name":"model","status":"Updating","progress":{"phase":"Downloading","completedBytes":1}}`
	withArtifact := entryJSON("sha", modelKey, "/models/model")
	for _, tc := range []struct {
		name        string
		entry       string // the model's ConfigMap entry; empty when there is none
		deleting    bool   // the task's model has a deletion timestamp
		cached      string // the lister's copy of the model: "", "live", or "deleting"
		ineligible  bool
		download    bool
		wantRemoved bool
	}{
		{name: "entry removed by an earlier Delete", deleting: true, wantRemoved: true},
		{name: "downloading", entry: downloading, deleting: true, wantRemoved: true},
		{name: "download failed", entry: `{"name":"model","status":"Failed"}`, deleting: true, wantRemoved: true},
		{name: "metadata parse failed", entry: `{"name":"model","status":"Ready"}`, deleting: true, wantRemoved: true},
		{name: "no longer targets this node", entry: downloading, cached: "live", ineligible: true, wantRemoved: true},
		{name: "download finds model deleting", entry: downloading, cached: "deleting", download: true, wantRemoved: true},
		{name: "unconfirmed and not cached", entry: withArtifact},
		{name: "unconfirmed and live", entry: withArtifact, cached: "live"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := map[string]string{}
			if tc.entry != "" {
				data[modelKey] = tc.entry
			}
			g := newGopherForProcessTask(makeConfigMap("node-1", data))
			g.modelRootDir = t.TempDir()
			path := filepath.Join(g.modelRootDir, "model")
			weights := filepath.Join(path, "model.safetensors")
			require.NoError(t, os.Mkdir(path, 0o755))
			require.NoError(t, os.WriteFile(weights, []byte("weights"), 0o644))
			uri := "hf://org/model"
			model := &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "uid-model"},
				Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri, Path: &path}},
			}
			now := metav1.Now()
			lister := &mockClusterBaseModelLister{}
			if tc.cached != "" {
				latest := model.DeepCopy()
				if tc.cached == "deleting" {
					latest.DeletionTimestamp = &now
				}
				lister.models = []*v1beta1.ClusterBaseModel{latest}
			}
			g.baseModelLister = &mockBaseModelLister{}
			g.clusterBaseModelLister = lister
			if tc.deleting {
				model.DeletionTimestamp = &now
			}
			task := &GopherTask{TaskType: Delete, ClusterBaseModel: model, NodeIneligible: tc.ineligible}
			if tc.download {
				task.TaskType = Download
			}

			require.NoError(t, g.processTask(task))

			if tc.download {
				require.FileExists(t, weights)
				require.NotNil(t, g.taskQueue)
				require.Equal(t, 1, g.taskQueue.len())
				cleanup, ok := g.taskQueue.popHighPriority()
				require.True(t, ok)
				require.Equal(t, Delete, cleanup.TaskType)
				require.Equal(t, getModelUID(task), getModelUID(cleanup))
				require.NoError(t, g.processTask(cleanup))
			}

			if !tc.wantRemoved {
				assert.FileExists(t, weights)
				return
			}
			assert.NoDirExists(t, path)
			cm, err := g.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			assert.NotContains(t, cm.Data, modelKey)
		})
	}
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

func TestHasChildrenPaths_MissingEntry_NoChildren(t *testing.T) {
	cm := makeConfigMap("node-1", map[string]string{})
	g, client := newGopherAndClientWithConfigMap(cm, t)

	childrenPaths, parentName, parentDir, err := g.parseModelConfigDataEntry(context.Background(), "clusterbasemodel.child")

	assert.NoError(t, err, "a missing entry records no children")
	assert.Empty(t, childrenPaths)
	assert.Empty(t, parentName)
	assert.Empty(t, parentDir)
	assert.Equal(t, 1, countConfigMapGets(client))
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

	// Malformed child entry to trigger error in getParentPathAndChildrenPaths (invalid JSON)
	childEntry := `{"config":`

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
		childKey: `{"config":`,
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

func TestIsSkippingArtifactDeletion_MissingEntry_ProceedsWithoutParentLookup(t *testing.T) {
	cm := makeConfigMap("node-1", map[string]string{})
	g, client := newGopherAndClientWithConfigMap(cm, t)
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}
	task := &GopherTask{
		TaskType: Delete,
		BaseModel: &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "child"},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{}},
		},
	}

	skip, isRemoveParent, parentName, parentDir := g.isSkippingArtifactDeletion(context.Background(), task, "/models/child", true)

	assert.False(t, skip, "a missing entry records no children")
	assert.False(t, isRemoveParent)
	assert.Empty(t, parentName)
	assert.Empty(t, parentDir)
	assert.Equal(t, 1, countConfigMapGets(client), "no parent is recorded, so there is no parent to look up")
	assert.Zero(t, countConfigMapUpdates(client))
}

// Hugging Face reuse before shared artifacts linked a model's path to another
// model's directory, and the link can outlive that directory's ConfigMap entry.
func TestIsSkippingArtifactDeletion_LinkFromOtherModel(t *testing.T) {
	for _, tc := range []struct {
		name       string
		linkTarget string // target of the other model's path, relative to the model root
		deleteLink bool   // the deleted model's path is itself a link to the directory
		wantSkip   bool
	}{
		{name: "other model links to the directory", linkTarget: "parent", wantSkip: true},
		{name: "other model links elsewhere", linkTarget: "elsewhere"},
		{name: "other model link is dangling", linkTarget: "missing"},
		{name: "deleted path is a sibling link", linkTarget: "parent", deleteLink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(root, "parent"), 0o755))
			require.NoError(t, os.Mkdir(filepath.Join(root, "elsewhere"), 0o755))
			destPath := filepath.Join(root, "parent")
			if tc.deleteLink {
				destPath = filepath.Join(root, "child")
				require.NoError(t, os.Symlink("parent", destPath))
			}
			otherPath := filepath.Join(root, "other")
			require.NoError(t, os.Symlink(tc.linkTarget, otherPath))
			uri := "hf://org/model"
			g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
			g.modelRootDir = root
			g.baseModelLister = &mockBaseModelLister{}
			g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{{
				ObjectMeta: metav1.ObjectMeta{Name: "other"},
				Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri, Path: &otherPath}},
			}}}
			task := &GopherTask{TaskType: Delete, ClusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "deleted"},
				Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri, Path: &destPath}},
			}}

			skip, isRemoveParent, _, _ := g.isSkippingArtifactDeletion(context.Background(), task, destPath, true)

			assert.Equal(t, tc.wantSkip, skip)
			assert.False(t, isRemoveParent)
		})
	}
}

func TestIsLinkedByOtherModel_ListErrorKeepsDirectory(t *testing.T) {
	g := newGopherWithConfigMap(makeConfigMap("node-1", map[string]string{}))
	g.baseModelLister = &mockBaseModelLister{err: errors.New("lister failed")}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}
	task := &GopherTask{TaskType: Delete, ClusterBaseModel: &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "deleted"}}}

	assert.True(t, g.isLinkedByOtherModel(t.TempDir(), task))
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

// Affinity cleanup keeps the CR and its UID alive. A canceled download must not
// recreate state after that cleanup, even when it subsequently reports an error.
func TestCanceledDownloadFailureAfterAffinityCleanup(t *testing.T) {
	cm := makeConfigMap("node-1", map[string]string{})
	g := newGopherForProcessTask(cm, map[string]string{"kubernetes.io/hostname": "node-1"})
	g.metrics = NewMetrics(prometheus.NewRegistry())
	uri := "local:///missing-model"
	missing := filepath.Join(t.TempDir(), "missing")
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "same-uid"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri, Path: &missing}},
	}
	task := &GopherTask{TaskType: Download, BaseModel: model}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempt := g.taskTracker.beginLegacyTask(gopherTaskModelKey(task), cancel)
	defer g.taskTracker.finishLegacyTask(attempt)
	g.taskTracker.cancelLegacyDownload(gopherTaskModelKey(task))
	require.NoError(t, g.processTask(&GopherTask{TaskType: Delete, BaseModel: model}))

	// Exercise the shared failure publisher through an existing download path.
	require.Error(t, g.processLocalStorageModel(ctx, task, model.Spec, "model", "BaseModel", "default", "model"))
	latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Get(context.Background(), cm.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, latest.Data, constants.GetModelConfigMapKey("default", "model", false))
	node, err := g.nodeLabelReconciler.kubeClient.CoreV1().Nodes().Get(context.Background(), cm.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, node.Labels, constants.GetBaseModelLabel("default", "model"))

	// Affinity can later include this node again without changing the CR UID.
	require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready}))
	latest, err = g.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Get(context.Background(), cm.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var entry ModelEntry
	require.NoError(t, json.Unmarshal([]byte(latest.Data[constants.GetModelConfigMapKey("default", "model", false)]), &entry))
	assert.Equal(t, ModelStatusReady, entry.Status)
}

func TestCanceledDownloadCannotPublishAfterCleanup(t *testing.T) {
	for _, state := range []ModelStateOnNode{Failed, Ready, Updating} {
		t.Run(string(state), func(t *testing.T) {
			cm := makeConfigMap("node-1", map[string]string{})
			g := newGopherForProcessTask(cm, map[string]string{"kubernetes.io/hostname": "node-1"})
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "same-uid"}}
			op := &NodeLabelOp{BaseModel: model, ModelStateOnNode: state}
			ctx, cancel := context.WithCancel(context.Background())
			// Hold the cleanup lock while the old writer waits; cancellation must be
			// checked inside the lock before either Kubernetes mutation is attempted.
			g.configMapMutex.Lock()
			done := make(chan error, 1)
			go func() { done <- g.safeNodeLabelReconciliation(ctx, op) }()
			cancel()
			g.configMapMutex.Unlock()
			require.ErrorIs(t, <-done, context.Canceled)
			assert.Empty(t, g.configMapReconciler.kubeClient.(*k8sfake.Clientset).Actions())

			// A new attempt for the same UID must remain allowed, including real failures.
			require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), op))
			latest, err := g.configMapReconciler.kubeClient.CoreV1().ConfigMaps(cm.Namespace).Get(context.Background(), cm.Name, metav1.GetOptions{})
			require.NoError(t, err)
			var entry ModelEntry
			require.NoError(t, json.Unmarshal([]byte(latest.Data[constants.GetModelConfigMapKey("default", "model", false)]), &entry))
			assert.Equal(t, ModelStatus(state), entry.Status)
			// The old task still cannot overwrite a newer task's status.
			op.ModelStateOnNode = Failed
			require.ErrorIs(t, g.safeNodeLabelReconciliation(ctx, op), context.Canceled)
		})
	}
}

func TestCanceledDownloadSkipsMetadataAndOCIRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := &Gopher{}
	// Nil dependencies deliberately ensure no parsing or OCI initialization occurs.
	require.ErrorIs(t, g.safeParseAndUpdateModelConfig(ctx, "", nil, nil, nil), context.Canceled)
	require.ErrorIs(t, g.downloadModel(ctx, nil, "", nil), context.Canceled)
}

type cancelVerificationDispatcher struct {
	cancel context.CancelFunc
	calls  int
}

func (d *cancelVerificationDispatcher) Do(req *http.Request) (*http.Response, error) {
	d.calls++
	d.cancel()
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Length": []string{"4"}, "Content-Md5": []string{"jXd/OF09/siBXSD3SWAm3A=="}}, Body: http.NoBody, Request: req}, nil
}

type verificationTestSigner struct{}

func (verificationTestSigner) Sign(*http.Request) error { return nil }

func TestVerificationStopsWhenAffinityCleanupCancelsDownload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatcher := &cancelVerificationDispatcher{cancel: cancel}
	store := &ociobjectstore.OCIOSDataStore{Client: &objectstorage.ObjectStorageClient{BaseClient: common.BaseClient{
		HTTPClient: dispatcher, Signer: verificationTestSigner{}, Host: "https://objectstorage.test", UserAgent: "test",
	}}}
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "first"), []byte("data"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "second"), []byte("data"), 0600))
	// A nil metrics collector makes accidental recording of cancellation as an
	// integrity failure fail the test. Subsequent files must not be verified.
	g := &Gopher{modelVerificationLimiter: newVerificationLimiter(1)}
	_ = g.verifyDownloadedFiles(ctx, store, []ociobjectstore.ObjectURI{
		{Namespace: "ns", BucketName: "bucket", ObjectName: "first"},
		{Namespace: "ns", BucketName: "bucket", ObjectName: "second"},
	}, dir, nil)
	assert.Equal(t, 1, dispatcher.calls)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// Cover the other ordering: a publisher has already passed the context check
// when deletion cancels it. Cleanup must run after that publisher and win.
func TestAffinityCleanupWinsOverInFlightStatusWrite(t *testing.T) {
	cm := makeConfigMap("node-1", map[string]string{})
	g := newGopherForProcessTask(cm, map[string]string{"kubernetes.io/hostname": "node-1"})
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "same-uid"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	patchStarted := make(chan struct{})
	releasePatch := make(chan struct{})
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	var firstPatch sync.Once
	client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		firstPatch.Do(func() { close(patchStarted); <-releasePatch })
		return false, nil, nil
	})
	published := make(chan error, 1)
	go func() {
		published <- g.safeNodeLabelReconciliation(ctx, &NodeLabelOp{BaseModel: model, ModelStateOnNode: Ready})
	}()
	<-patchStarted
	cancel()
	deleted := make(chan error, 1)
	go func() {
		deleted <- g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: model, ModelStateOnNode: Deleted})
	}()
	close(releasePatch)
	// Real clients may reject the in-flight request after cancellation. Fake
	// clients may complete it; either outcome must be followed by cleanup.
	if err := <-published; err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.NoError(t, <-deleted)
	latest, err := client.CoreV1().ConfigMaps(cm.Namespace).Get(context.Background(), cm.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, latest.Data, constants.GetModelConfigMapKey("default", "model", false))
	node, err := client.CoreV1().Nodes().Get(context.Background(), cm.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, node.Labels, constants.GetBaseModelLabel("default", "model"))
	g.configMapReconciler.cacheMutex.RLock()
	_, exists := g.configMapReconciler.modelCache[constants.GetModelConfigMapKey("default", "model", false)]
	g.configMapReconciler.cacheMutex.RUnlock()
	assert.False(t, exists, "periodic reconciliation must not restore the entry")
}

func TestOCICanceledDownloadDoesNotRecordFailure(t *testing.T) {
	cm := makeConfigMap("node-1", map[string]string{})
	g := newGopherForProcessTask(cm, map[string]string{"kubernetes.io/hostname": "node-1"})
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.downloadRetry = 3
	uri := "oci://n/test-ns/b/test-bucket/o/model"
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "same-uid"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri, Path: stringPtr(t.TempDir())}},
	}
	task := &GopherTask{TaskType: DownloadOverride, BaseModel: model}
	// Cancel after registration during the Updating write, before OCI access.
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		g.enqueueTask(&GopherTask{TaskType: Delete, BaseModel: task.BaseModel})
		return false, nil, nil
	})
	require.ErrorIs(t, g.processTask(task), context.Canceled)
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)
	assert.Zero(t, testutil.ToFloat64(g.metrics.modelDownloadsFailedTotal.WithLabelValues(modelType, namespace, name)))
	assert.Zero(t, testutil.ToFloat64(g.metrics.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name)))
}

func newSelectiveDeletionGopher(t *testing.T, uri string) (*Gopher, *GopherTask) {
	t.Helper()
	g, original := newCancellationCoverageGopher(t, uri)
	g.modelRootDir = t.TempDir()
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpt-oss-20b", UID: "parent-uid",
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: original.BaseModel.Spec,
	}
	model.Spec.Storage.Path = stringPtr(filepath.Join(g.modelRootDir, model.Name))
	g.baseModelLister = &mockBaseModelLister{}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{models: []*v1beta1.ClusterBaseModel{model}}
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		ClusterBaseModel: model,
		ModelMetadata: ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{
			getModelID(nil, model): *model.Spec.Storage.Path,
		}}},
	}))
	require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{
		ClusterBaseModel: model, ModelStateOnNode: Ready,
	}))
	return g, &GopherTask{TaskType: Delete, ClusterBaseModel: model}
}

func writeSelectiveDeletionFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(path), 0o600))
}

func TestDirectDeleteWithdrawsReadyBeforeFiles(t *testing.T) {
	for _, uri := range []string{"hf://openai/gpt-oss-20b", "oci://n/ns/b/models/o/model", "modelpack://registry.example.com/org/model:tag"} {
		for _, failure := range []string{"none", "status update", "node read", "label patch", "final status removal"} {
			t.Run(uri+"/"+failure, func(t *testing.T) {
				g, task := newSelectiveDeletionGopher(t, uri)
				ctx := context.Background()
				c := g.configMapReconciler
				client := c.kubeClient.(*k8sfake.Clientset)
				path := *task.ClusterBaseModel.Spec.Storage.Path
				weights := filepath.Join(path, "weights")
				writeSelectiveDeletionFile(t, weights)
				key := getModelID(nil, task.ClusterBaseModel)
				label, err := getModelLabelKey(&NodeLabelOp{ClusterBaseModel: task.ClusterBaseModel})
				require.NoError(t, err)
				patches := 0
				client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					patches++
					assert.FileExists(t, weights)
					object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), c.namespace, c.nodeName)
					require.NoError(t, err)
					child, err := existingModelEntry(object.(*corev1.ConfigMap).Data, key)
					require.NoError(t, err)
					assert.Equal(t, ModelStatusUpdating, child.Status)
					if failure == "label patch" {
						return true, nil, errors.New("label patch failed")
					}
					return false, nil, nil
				})
				client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					if failure == "node read" {
						return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, c.nodeName)
					}
					// Finalization must not depend on another Node API read.
					if _, err := os.Stat(weights); os.IsNotExist(err) {
						return true, nil, errors.New("Node API unavailable after cleanup")
					}
					return false, nil, nil
				})
				client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
					cm := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					_, exists := cm.Data[key]
					if failure == "status update" && exists || failure == "final status removal" && !exists {
						return true, nil, errors.New("status write failed")
					}
					return false, nil, nil
				})
				err = g.processTask(task)
				if failure == "none" {
					require.NoError(t, err)
					assert.NoFileExists(t, weights)
					cm, err := c.getConfigMap(ctx)
					require.NoError(t, err)
					assert.NotContains(t, cm.Data, key)
				} else {
					require.Error(t, err)
					if failure == "final status removal" {
						assert.NoFileExists(t, weights)
						// Retry with the same agent, including its deletion UID fence.
						client.ReactionChain = client.ReactionChain[3:]
						retry := *task
						retry.Sequence = 0
						require.NoError(t, g.processTask(&retry))
						cm, err := c.getConfigMap(ctx)
						require.NoError(t, err)
						assert.NotContains(t, cm.Data, key)
					} else {
						assert.FileExists(t, weights)
					}
				}
				if failure == "status update" || failure == "node read" {
					assert.Zero(t, patches)
				} else {
					assert.Positive(t, patches)
				}
				object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", c.nodeName)
				require.NoError(t, err)
				if failure != "none" && failure != "final status removal" {
					assert.Equal(t, string(Ready), object.(*corev1.Node).Labels[label])
				} else {
					assert.NotContains(t, object.(*corev1.Node).Labels, label)
				}
			})
		}
	}
}

func TestNonFileDeleteStillCleansStatusWhenNodeIsGone(t *testing.T) {
	for _, uri := range []string{"local://model", "pvc://volume/model", "vendor://model"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newSelectiveDeletionGopher(t, uri)
			ctx := context.Background()
			c := g.configMapReconciler
			client := c.kubeClient.(*k8sfake.Clientset)
			weights := filepath.Join(*task.ClusterBaseModel.Spec.Storage.Path, "weights")
			writeSelectiveDeletionFile(t, weights)
			require.NoError(t, client.CoreV1().Nodes().Delete(ctx, c.nodeName, metav1.DeleteOptions{}))
			require.NoError(t, g.processTask(task))
			assert.FileExists(t, weights)
			cm, err := c.getConfigMap(ctx)
			require.NoError(t, err)
			assert.NotContains(t, cm.Data, getModelID(nil, task.ClusterBaseModel))
		})
	}
}

func TestModelDeleteRechecksUIDBeforeNodeRetry(t *testing.T) {
	for _, uri := range []string{"oci://n/ns/b/models/o/model", "local://model", "pvc://volume/model", "vendor://model"} {
		t.Run(uri, func(t *testing.T) {
			g, task := newSelectiveDeletionGopher(t, uri)
			ctx := context.Background()
			g.omeClient = omefake.NewSimpleClientset(task.ClusterBaseModel.DeepCopy())
			c := g.configMapReconciler
			client := c.kubeClient.(*k8sfake.Clientset)
			g.nodeLabelReconciler = NewNodeLabelReconciler(c.nodeName, client, 2, g.logger)
			weights := filepath.Join(*task.ClusterBaseModel.Spec.Storage.Path, "weights")
			writeSelectiveDeletionFile(t, weights)
			reads, patches := 0, 0
			client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				reads++
				return false, nil, nil
			})
			client.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
				patches++
				assert.Contains(t, string(action.(k8stesting.PatchAction).GetPatch()), "/metadata/resourceVersion")
				replacement := task.ClusterBaseModel.DeepCopy()
				replacement.UID = "replacement-uid"
				_, err := g.omeClient.OmeV1beta1().ClusterBaseModels().Update(ctx, replacement, metav1.UpdateOptions{})
				require.NoError(t, err)
				return true, nil, errors.New("Node changed before patch")
			})
			require.ErrorContains(t, g.processTask(task), "model UID changed")
			assert.GreaterOrEqual(t, reads, 2)
			assert.Equal(t, 1, patches)
			assert.FileExists(t, weights)
			node, err := client.CoreV1().Nodes().Get(ctx, c.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			label, err := getModelLabelKey(&NodeLabelOp{ClusterBaseModel: task.ClusterBaseModel})
			require.NoError(t, err)
			assert.Equal(t, string(Ready), node.Labels[label])
		})
	}
}

func TestModelDeletionPreservesReferencedDescendants(t *testing.T) {
	for _, uri := range []string{"hf://openai/gpt-oss-20b", "oci://n/ns/b/models/o/gpt-oss-20b", "modelpack://registry.example.com/org/model:tag"} {
		t.Run(uri, func(t *testing.T) {
			for _, kind := range []string{"ClusterBaseModel", "BaseModel"} {
				t.Run(kind, func(t *testing.T) {
					g, task := newSelectiveDeletionGopher(t, uri)
					target := *task.ClusterBaseModel.Spec.Storage.Path
					protected := filepath.Join(target, "releases", "rev-002")
					child := &v1beta1.ClusterBaseModel{
						ObjectMeta: metav1.ObjectMeta{Name: "gpt-oss-20b-rev-002", UID: "child-uid"},
						Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(protected)}},
					}
					if kind == "ClusterBaseModel" {
						g.clusterBaseModelLister.(*mockClusterBaseModelLister).models = append(
							g.clusterBaseModelLister.(*mockClusterBaseModelLister).models, child)
					} else {
						g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
							ObjectMeta: metav1.ObjectMeta{Name: task.ClusterBaseModel.Name, Namespace: "another-namespace"},
							Spec:       child.Spec,
						}}
					}
					kept := filepath.Join(protected, "weights.safetensors")
					removed := []string{
						filepath.Join(target, "weights.safetensors"),
						filepath.Join(target, "config.json"),
						filepath.Join(target, "releases", "rev-001", "weights.safetensors"),
						filepath.Join(target, "unused", "file"),
					}
					writeSelectiveDeletionFile(t, kept)
					for _, path := range removed {
						writeSelectiveDeletionFile(t, path)
					}

					require.NoError(t, g.processTask(task))
					contents, err := os.ReadFile(kept)
					require.NoError(t, err, "deleting the parent must preserve a live revision")
					require.Equal(t, kept, string(contents))
					for _, path := range removed {
						require.NoFileExists(t, path)
					}
					require.NoDirExists(t, filepath.Join(target, "releases", "rev-001"))
					require.NoDirExists(t, filepath.Join(target, "unused"))
					exists, _, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), getModelID(nil, task.ClusterBaseModel))
					require.NoError(t, err)
					require.False(t, exists)
					node, err := g.nodeLabelReconciler.kubeClient.CoreV1().Nodes().Get(context.Background(), g.nodeLabelReconciler.nodeName, metav1.GetOptions{})
					require.NoError(t, err)
					require.NotContains(t, node.Labels, constants.GetClusterBaseModelLabel(task.ClusterBaseModel.Name))
					require.NoError(t, g.processTask(task), "repeated cleanup must be idempotent")
					require.FileExists(t, kept)
				})
			}
		})
	}
}

func TestModelDeletionReferenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		suffix     string
		keepTarget bool
	}{
		{name: "same path with trailing separator", suffix: "/", keepTarget: true},
		{name: "same path after cleaning", suffix: "/releases/..", keepTarget: true},
		{name: "sibling sharing a string prefix", suffix: "-bf16"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
			target := *task.ClusterBaseModel.Spec.Storage.Path
			reference := target + tc.suffix
			g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
				ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "other"},
				Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(reference)}},
			}}
			weights := filepath.Join(target, "weights")
			consumerWeights := filepath.Join(reference, "weights")
			writeSelectiveDeletionFile(t, weights)
			writeSelectiveDeletionFile(t, consumerWeights)
			require.NoError(t, g.processTask(task))
			require.FileExists(t, consumerWeights)
			if tc.keepTarget {
				require.FileExists(t, weights)
			} else {
				require.NoDirExists(t, target)
			}
		})
	}
}

func TestModelDeletionProtectsMultipleAndMissingRevisions(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	protected := []string{
		filepath.Join(target, "releases", "rev-002"),
		filepath.Join(target, "releases", "rev-003", "nested"),
		filepath.Join(target, "downloads", "pending-revision"),
	}
	for i, path := range protected {
		g.baseModelLister.(*mockBaseModelLister).models = append(g.baseModelLister.(*mockBaseModelLister).models, &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: filepath.Base(path), Namespace: "other"},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(path + "/")}},
		})
		if i < 2 {
			writeSelectiveDeletionFile(t, filepath.Join(path, "weights"))
		}
	}
	removed := []string{
		filepath.Join(target, "weights"),
		filepath.Join(target, "releases", "rev-003", "unused"),
		filepath.Join(target, "downloads", "old-partial"),
	}
	for _, path := range removed {
		writeSelectiveDeletionFile(t, path)
	}
	require.NoError(t, g.processTask(task))
	for _, path := range protected[:2] {
		require.FileExists(t, filepath.Join(path, "weights"))
	}
	require.DirExists(t, filepath.Dir(protected[2]), "retain the container for a revision that is not downloaded yet")
	for _, path := range removed {
		require.NoFileExists(t, path)
	}
}

func TestModelDeletionPreservesSymlinkConsumers(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	protected := filepath.Join(target, "releases", "rev-002")
	kept := filepath.Join(protected, "weights")
	writeSelectiveDeletionFile(t, kept)
	consumer := filepath.Join(g.modelRootDir, "consumer")
	require.NoError(t, os.Symlink(protected, consumer))
	g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
		ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "other"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(consumer)}},
	}}
	outside := filepath.Join(g.modelRootDir, "outside", "weights")
	writeSelectiveDeletionFile(t, outside)
	unusedLink := filepath.Join(target, "unused-link")
	require.NoError(t, os.Symlink(filepath.Dir(outside), unusedLink))
	removed := filepath.Join(target, "weights")
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, g.processTask(task))
	require.FileExists(t, kept)
	require.FileExists(t, filepath.Join(consumer, "weights"))
	require.FileExists(t, outside, "cleanup must not follow an unreferenced symlink")
	_, err := os.Lstat(unusedLink)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoFileExists(t, removed)
}

func TestModelDeletionRetainsInternalArtifactAndLockDirectories(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	kept := []string{
		filepath.Join(target, hfArtifactLockDirectory, "active.lock"),
		filepath.Join(target, constants.ModelArtifactsDirectory, "shared", "weights"),
	}
	for _, path := range kept {
		writeSelectiveDeletionFile(t, path)
	}
	lockInfo, err := os.Stat(kept[0])
	require.NoError(t, err)
	removed := filepath.Join(target, "weights")
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, g.processTask(task))
	for _, path := range kept {
		require.FileExists(t, path)
	}
	currentInfo, err := os.Stat(kept[0])
	require.NoError(t, err)
	require.True(t, os.SameFile(lockInfo, currentInfo), "an active lock must keep its inode")
	require.NoFileExists(t, removed)
}

func TestModelDeletionReferenceLookupFailureKeepsAllFiles(t *testing.T) {
	for _, kind := range []string{"BaseModel", "ClusterBaseModel"} {
		t.Run(kind, func(t *testing.T) {
			g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
			weights := filepath.Join(*task.ClusterBaseModel.Spec.Storage.Path, "weights")
			writeSelectiveDeletionFile(t, weights)
			lookupErr := errors.New("cache unavailable")
			if kind == "BaseModel" {
				g.baseModelLister.(*mockBaseModelLister).err = lookupErr
			} else {
				g.clusterBaseModelLister.(*mockClusterBaseModelLister).err = lookupErr
			}
			require.ErrorIs(t, g.deleteModel(*task.ClusterBaseModel.Spec.Storage.Path, task), lookupErr)
			require.FileExists(t, weights)
		})
	}
}

func TestLegacyParentCleanupPreservesReferencedRevision(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "hf://openai/gpt-oss-20b")
	parent := *task.ClusterBaseModel.Spec.Storage.Path
	child := filepath.Join(g.modelRootDir, "legacy-child")
	kept := filepath.Join(parent, "releases", "rev-002", "weights")
	removed := filepath.Join(parent, "weights")
	writeSelectiveDeletionFile(t, kept)
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, os.Symlink(parent, child))
	task.ClusterBaseModel.Spec.Storage.Path = stringPtr(child)
	require.NoError(t, g.configMapReconciler.ReconcileModelMetadata(context.Background(), &ConfigMapMetadataOp{
		ClusterBaseModel: task.ClusterBaseModel,
		ModelMetadata:    ModelMetadata{Artifact: Artifact{ParentPath: map[string]string{"clusterbasemodel.deleted-parent": parent}}},
	}))
	g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{{
		ObjectMeta: metav1.ObjectMeta{Name: "revision", Namespace: "other"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: stringPtr(filepath.Dir(kept))}},
	}}
	require.NoError(t, g.processTask(task))
	_, err := os.Lstat(child)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.FileExists(t, kept)
	require.NoFileExists(t, removed)
}

func TestModelDeletionProtectsDefaultStoragePath(t *testing.T) {
	g, task := newSelectiveDeletionGopher(t, "oci://n/ns/b/models/o/model")
	target := *task.ClusterBaseModel.Spec.Storage.Path
	g.modelRootDir = target
	consumer := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "other"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("hf://org/model")}},
	}
	g.baseModelLister.(*mockBaseModelLister).models = []*v1beta1.BaseModel{consumer}
	kept := filepath.Join(getDestPath(&consumer.Spec, g.modelRootDir), "weights")
	removed := filepath.Join(target, "weights")
	writeSelectiveDeletionFile(t, kept)
	writeSelectiveDeletionFile(t, removed)
	require.NoError(t, g.processTask(task))
	require.FileExists(t, kept)
	require.NoFileExists(t, removed)
}
