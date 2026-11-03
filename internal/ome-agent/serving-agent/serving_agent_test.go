package serving_agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

// MockCasperDataStore mocks the OCIOSDataStore for testing
type MockCasperDataStore struct {
	mock.Mock
	*ociobjectstore.OCIOSDataStore // embedding for type compatibility
}

func TestNewServingSidecar(t *testing.T) {
	mockLogger := testingPkg.SetupMockLogger()
	mockDataStore := &ociobjectstore.OCIOSDataStore{}

	config := &Config{
		AnotherLogger:                    mockLogger,
		FineTunedWeightInfoFilePath:      "/test/path/weights.json",
		UnzippedFineTunedWeightDirectory: "/test/path/unzipped",
		ZippedFineTunedWeightDirectory:   "/test/path/zipped",
		ObjectStorageDataStore:           mockDataStore,
	}

	sidecar, err := NewServingSidecar(config)

	assert.NoError(t, err)
	assert.NotNil(t, sidecar)
	assert.Equal(t, mockLogger, sidecar.logger)
	assert.Equal(t, *config, sidecar.Config)
}

func TestReadObjectURIsFromFile(t *testing.T) {
	tempFile, cleanup, err := testingPkg.TempFile()
	require.NoError(t, err)
	defer cleanup()

	// Keys follow ObjectURI's JSON tags.
	testData := []map[string]interface{}{
		{
			"namespace":   "test-namespace",
			"bucket_name": "test-bucket",
			"object_name": "model1.zip",
		},
		{
			"namespace":   "test-namespace",
			"bucket_name": "test-bucket",
			"object_name": "model2.zip",
		},
	}

	// Write test data to file
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)
	_, err = tempFile.Write(jsonData)
	require.NoError(t, err)
	err = tempFile.Close()
	require.NoError(t, err)

	// Call the function under test
	uris, names, err := readObjectURIsFromFile(tempFile.Name())

	// Assertions
	assert.NoError(t, err)
	assert.Len(t, uris, 2)
	assert.Len(t, names, 2)

	// Verify URI details
	assert.Equal(t, "test-namespace", uris[0].Namespace)
	assert.Equal(t, "test-bucket", uris[0].BucketName)
	assert.Equal(t, "model1.zip", uris[0].ObjectName)

	assert.Equal(t, "test-namespace", uris[1].Namespace)
	assert.Equal(t, "test-bucket", uris[1].BucketName)
	assert.Equal(t, "model2.zip", uris[1].ObjectName)

	// Verify model names
	assert.Equal(t, "model1.zip", names[0])
	assert.Equal(t, "model2.zip", names[1])
}

func TestReadObjectURIsFromFile_InvalidJSON(t *testing.T) {
	// Create a temporary file with invalid JSON
	tempFile, cleanup, err := testingPkg.TempFile()
	require.NoError(t, err)
	defer cleanup()

	// Write invalid JSON to file
	_, err = tempFile.WriteString("invalid json")
	require.NoError(t, err)
	err = tempFile.Close()
	require.NoError(t, err)

	// Call the function under test
	_, _, err = readObjectURIsFromFile(tempFile.Name())

	// Assertions
	assert.Error(t, err)
}

func TestReadObjectURIsFromFile_FileNotFound(t *testing.T) {
	// Call the function with a non-existent file
	_, _, err := readObjectURIsFromFile("/non/existent/file.json")

	// Assertions
	assert.Error(t, err)
}

func TestGetExistingFtModelNamesFromDir(t *testing.T) {
	tempDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	// A downloaded zipped object has no file extension, so only model1 and
	// model2 count: the .zip file and the directory are left out.
	for _, name := range []string{"model1", "model2", "notes.zip"} {
		f, err := os.Create(filepath.Join(tempDir, name))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	require.NoError(t, os.Mkdir(filepath.Join(tempDir, "unzipped"), 0o755))

	names, err := getExistingFtModelNamesFromDir(tempDir)

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"model1", "model2"}, names)
}

func TestGetExistingFtModelNamesFromDir_EmptyDir(t *testing.T) {
	// Create a temporary directory
	tempDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	// Call the function under test
	modelNames, err := getExistingFtModelNamesFromDir(tempDir)

	// Assertions
	assert.NoError(t, err)
	assert.Empty(t, modelNames)
}

func TestGetExistingFtModelNamesFromDir_DirNotFound(t *testing.T) {
	// Call the function with a non-existent directory
	_, err := getExistingFtModelNamesFromDir("/non/existent/dir")

	// Assertions
	assert.Error(t, err)
}

func TestFindModelNameDifferences(t *testing.T) {
	tests := []struct {
		name                   string
		currentModels          []string
		existingModels         []string
		expectedModelsToAdd    map[string]bool
		expectedModelsToDelete map[string]bool
	}{
		{
			name:                   "add new models",
			currentModels:          []string{"model1.zip", "model2.zip", "model3.zip"},
			existingModels:         []string{"model1.zip"},
			expectedModelsToAdd:    map[string]bool{"model2.zip": true, "model3.zip": true},
			expectedModelsToDelete: map[string]bool{},
		},
		{
			name:                   "delete old models",
			currentModels:          []string{"model1.zip"},
			existingModels:         []string{"model1.zip", "model2.zip", "model3.zip"},
			expectedModelsToAdd:    map[string]bool{},
			expectedModelsToDelete: map[string]bool{"model2.zip": true, "model3.zip": true},
		},
		{
			name:                   "add and delete models",
			currentModels:          []string{"model1.zip", "model3.zip", "model4.zip"},
			existingModels:         []string{"model1.zip", "model2.zip", "model3.zip"},
			expectedModelsToAdd:    map[string]bool{"model4.zip": true},
			expectedModelsToDelete: map[string]bool{"model2.zip": true},
		},
		{
			name:                   "identical models",
			currentModels:          []string{"model1.zip", "model2.zip"},
			existingModels:         []string{"model1.zip", "model2.zip"},
			expectedModelsToAdd:    map[string]bool{},
			expectedModelsToDelete: map[string]bool{},
		},
		{
			name:                   "empty current models",
			currentModels:          []string{},
			existingModels:         []string{"model1.zip", "model2.zip"},
			expectedModelsToAdd:    map[string]bool{},
			expectedModelsToDelete: map[string]bool{"model1.zip": true, "model2.zip": true},
		},
		{
			name:                   "empty existing models",
			currentModels:          []string{"model1.zip", "model2.zip"},
			existingModels:         []string{},
			expectedModelsToAdd:    map[string]bool{"model1.zip": true, "model2.zip": true},
			expectedModelsToDelete: map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the function under test
			modelsToAdd, modelsToDelete := findModelNameDifferences(tt.currentModels, tt.existingModels)

			// Assertions
			assert.Equal(t, tt.expectedModelsToAdd, modelsToAdd)
			assert.Equal(t, tt.expectedModelsToDelete, modelsToDelete)
		})
	}
}

func TestDeleteFilesWithMatchingString(t *testing.T) {
	// Create a temporary directory with test files
	tempDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	// Create test structure
	err = os.MkdirAll(filepath.Join(tempDir, "subdir"), 0755)
	require.NoError(t, err)

	// Create files that should be deleted
	filesToDelete := []string{
		filepath.Join(tempDir, "model1_file1.txt"),
		filepath.Join(tempDir, "model1_file2.bin"),
		filepath.Join(tempDir, "subdir", "model1_file3.txt"),
	}

	// Create files that should not be deleted
	filesToKeep := []string{
		filepath.Join(tempDir, "model2_file1.txt"),
		filepath.Join(tempDir, "file3.txt"),
		filepath.Join(tempDir, "subdir", "model2_file2.bin"),
	}

	// Create all files
	for _, file := range append(filesToDelete, filesToKeep...) {
		err = os.WriteFile(file, []byte("test content"), 0644)
		require.NoError(t, err)
	}

	// Call the function under test
	err = deleteFilesWithMatchingString(tempDir, "model1")

	// Assertions
	assert.NoError(t, err)

	// Check deleted files
	for _, file := range filesToDelete {
		_, err := os.Stat(file)
		assert.True(t, os.IsNotExist(err), "File should be deleted: %s", file)
	}

	// Check kept files
	for _, file := range filesToKeep {
		_, err := os.Stat(file)
		assert.NoError(t, err, "File should exist: %s", file)
	}
}

func TestWatchFileChanges(t *testing.T) {
	// Setup
	mockLogger := testingPkg.SetupMockLogger()
	sidecar := &ServingSidecar{
		logger: mockLogger,
	}

	// Create a temporary directory for testing
	tempDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	// Create a watcher
	watcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)
	defer watcher.Close()

	// Call the function under test
	changeDetected := sidecar.watchFileChanges(watcher, tempDir)

	// Create a file to trigger the watcher
	testFile := filepath.Join(tempDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test content"), 0644)
	require.NoError(t, err)

	// Wait for the event to be processed
	select {
	case change := <-changeDetected:
		assert.True(t, change)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for file change event")
	}
}

// TestServingSidecar_Start is a basic test for the Start method
// This is limited as it involves goroutines and signal handling
func TestServingSidecar_Start_Basic(t *testing.T) {
	// This is a minimal test since Start() involves goroutines and signal handling
	// We'll just verify it doesn't panic when called with mocked dependencies

	mockLogger := testingPkg.SetupMockLogger()
	// Create a properly initialized mock
	mockDataStore := &MockCasperDataStore{
		OCIOSDataStore: &ociobjectstore.OCIOSDataStore{},
	}

	// Create temporary directories for testing
	tempDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	infoFilePath := filepath.Join(tempDir, "info.json")
	unzippedDir := filepath.Join(tempDir, "unzipped")
	zippedDir := filepath.Join(tempDir, "zipped")

	// Create directories
	err = os.MkdirAll(unzippedDir, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(zippedDir, 0755)
	require.NoError(t, err)

	// Create empty info file with valid JSON
	emptyInfoContent := "[]"
	err = os.WriteFile(infoFilePath, []byte(emptyInfoContent), 0644)
	require.NoError(t, err)

	// Setup sidecar
	sidecar := &ServingSidecar{
		logger: mockLogger,
		Config: Config{
			AnotherLogger:                    mockLogger,
			FineTunedWeightInfoFilePath:      infoFilePath,
			UnzippedFineTunedWeightDirectory: unzippedDir,
			ZippedFineTunedWeightDirectory:   zippedDir,
			ObjectStorageDataStore:           mockDataStore.OCIOSDataStore, // Use embedded type for compatibility
		},
	}

	// We can't test the full Start method as it runs indefinitely
	// Instead, we'll test that applyFinetunedModelChanges doesn't panic
	// This is a compromise
	assert.NotPanics(t, func() {
		sidecar.applyFinetunedModelChanges()
	})
}
