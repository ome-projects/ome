package replicator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

func TestPVCToPVCReplicator_Replicate_Success(t *testing.T) {
	// Create temporary directories for testing
	tmpDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	// Create source and target directories
	// The replicator expects the path to be: LocalPath/pvcName/pvcPath
	sourceDir := filepath.Join(tmpDir, "pvcPath1")
	targetDir := filepath.Join(tmpDir, "targetPVCName", "pvcPath2")

	// Create source directory structure
	err = os.MkdirAll(sourceDir, 0755)
	require.NoError(t, err)

	// Create test files in source directory
	testFiles := map[string]string{
		"file1.txt":        "content1",
		"file2.txt":        "content2",
		"subdir/file3.txt": "content3",
	}

	for filename, content := range testFiles {
		filePath := filepath.Join(sourceDir, filename)
		// Ensure parent directory exists
		err = os.MkdirAll(filepath.Dir(filePath), 0755)
		require.NoError(t, err)

		err = os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)
	}

	// Create target directory
	err = os.MkdirAll(filepath.Dir(targetDir), 0755)
	require.NoError(t, err)

	replicator := &PVCToPVCReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath:   tmpDir,
			SourceFiles: afero.NewOsFs().(*afero.OsFs),
			TargetFiles: afero.NewOsFs().(*afero.OsFs),
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "sourcePVCName",
				Prefix:     "pvcPath1",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "targetPVCName",
				Prefix:     "pvcPath2",
			},
		},
	}

	// Execute replication
	err = replicator.Replicate([]common.ReplicationObject{})

	// Verify no error occurred
	assert.NoError(t, err)

	// Verify files were copied correctly
	for filename, expectedContent := range testFiles {
		targetFilePath := filepath.Join(targetDir, filename)

		// Check if file exists
		_, err = os.Stat(targetFilePath)
		assert.NoError(t, err, "Target file %s should exist", filename)

		// Check file content
		content, err := os.ReadFile(targetFilePath)
		assert.NoError(t, err)
		assert.Equal(t, expectedContent, string(content), "File content should match for %s", filename)
	}
}

func TestPVCToPVCReplicator_Replicate_WalkFunctionCalledCorrectly(t *testing.T) {
	// Create temporary directories for testing
	tmpDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	// Create source and target directories
	sourceDir := filepath.Join(tmpDir, "pvcPath1")
	targetDir := filepath.Join(tmpDir, "targetPVCName", "pvcPath2")

	// Create source directory structure
	err = os.MkdirAll(sourceDir, 0755)
	require.NoError(t, err)

	// Create test files in source directory
	testFiles := map[string]string{
		"file1.txt":        "content1",
		"file2.txt":        "content2",
		"subdir/file3.txt": "content3",
	}

	for filename, content := range testFiles {
		filePath := filepath.Join(sourceDir, filename)
		// Ensure parent directory exists
		err = os.MkdirAll(filepath.Dir(filePath), 0755)
		require.NoError(t, err)

		err = os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)
	}

	// Create target directory
	err = os.MkdirAll(filepath.Dir(targetDir), 0755)
	require.NoError(t, err)

	replicator := &PVCToPVCReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath:   tmpDir,
			SourceFiles: afero.NewOsFs().(*afero.OsFs),
			TargetFiles: afero.NewOsFs().(*afero.OsFs),
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "sourcePVCName",
				Prefix:     "pvcPath1",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "targetPVCName",
				Prefix:     "pvcPath2",
			},
		},
	}

	// Execute replication
	err = replicator.Replicate([]common.ReplicationObject{})

	// Verify no error occurred
	assert.NoError(t, err)

	// Verify that the walk function was called correctly by checking the results
	// The walk function should have visited all files and directories

	// Verify files were copied correctly (this indirectly verifies walk was called)
	for filename, expectedContent := range testFiles {
		targetFilePath := filepath.Join(targetDir, filename)

		// Check if file exists
		_, err = os.Stat(targetFilePath)
		assert.NoError(t, err, "Target file %s should exist", filename)

		// Check file content
		content, err := os.ReadFile(targetFilePath)
		assert.NoError(t, err)
		assert.Equal(t, expectedContent, string(content), "File content should match for %s", filename)
	}

	// Verify that the source directory structure was walked correctly
	// by checking that all expected files exist in the target
	expectedPaths := []string{
		filepath.Join(targetDir, "file1.txt"),
		filepath.Join(targetDir, "file2.txt"),
		filepath.Join(targetDir, "subdir", "file3.txt"),
	}

	for _, expectedPath := range expectedPaths {
		_, err = os.Stat(expectedPath)
		assert.NoError(t, err, "Expected path %s should exist after replication", expectedPath)
	}
}

func TestPVCToPVCReplicator_Replicate_SameSourceAndTarget(t *testing.T) {
	replicator := &PVCToPVCReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath:   "/tmp",
			SourceFiles: afero.NewOsFs().(*afero.OsFs),
			TargetFiles: afero.NewOsFs().(*afero.OsFs),
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "pvcName",
				Prefix:     "pvcPath",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace", // Same namespace
				BucketName: "pvcName",      // Same bucket
				Prefix:     "pvcPath",      // Same prefix
			},
		},
	}

	// Execute replication
	err := replicator.Replicate([]common.ReplicationObject{})

	// Should not error and should skip replication
	assert.NoError(t, err)
}

func TestPVCToPVCReplicator_Replicate_SourceDirectoryNotExists(t *testing.T) {
	tmpDir, cleanup, err := testingPkg.TempDir()
	require.NoError(t, err)
	defer cleanup()

	replicator := &PVCToPVCReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath:   tmpDir,
			SourceFiles: afero.NewOsFs().(*afero.OsFs),
			TargetFiles: afero.NewOsFs().(*afero.OsFs),
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "nonexistent-pvc",
				Prefix:     "nonexistent-pvcPath",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "pvcName",
				Prefix:     "pvcPath",
			},
		},
	}

	// Execute replication
	err = replicator.Replicate([]common.ReplicationObject{})

	// Should return an error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error accessing")
}

func TestPVCToPVCReplicator_Replicate_FileSystemError(t *testing.T) {
	// For this test, we'll use a real file system but test with a non-existent path
	// which will cause the Walk function to fail
	replicator := &PVCToPVCReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath:   "/tmp",
			SourceFiles: afero.NewOsFs().(*afero.OsFs),
			TargetFiles: afero.NewOsFs().(*afero.OsFs),
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "nonexistent-pvc",
				Prefix:     "nonexistent-pvcPath",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "pvcNamespace",
				BucketName: "pvcName",
				Prefix:     "pvcPath",
			},
		},
	}

	// Execute replication
	err := replicator.Replicate([]common.ReplicationObject{})

	// Should return an error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replication failed")
}
