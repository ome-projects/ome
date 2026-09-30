package ociobjectstore

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitToParts(t *testing.T) {
	source := ObjectURI{
		Namespace:  "test-namespace",
		BucketName: "test-bucket",
		ObjectName: "test-object.txt",
	}

	t.Run("Small file single part", func(t *testing.T) {
		totalParts := 1
		partSize := 1024 * 1024  // 1MB
		objectSize := 512 * 1024 // 512KB

		parts := splitToParts(context.Background(), totalParts, partSize, objectSize, source)

		var collectedParts []*PrepareDownloadPart
		for part := range parts {
			collectedParts = append(collectedParts, part)
		}

		assert.Len(t, collectedParts, 1)
		assert.Equal(t, int64(0), collectedParts[0].offset)
		assert.Equal(t, int64(objectSize), collectedParts[0].size)
		assert.Equal(t, "bytes=0-524287", collectedParts[0].byteRange)
		assert.Equal(t, source.Namespace, collectedParts[0].namespace)
		assert.Equal(t, source.BucketName, collectedParts[0].bucket)
		assert.Equal(t, source.ObjectName, collectedParts[0].object)
	})

	t.Run("Large file multiple parts", func(t *testing.T) {
		totalParts := 3
		partSize := 1024 * 1024         // 1MB
		objectSize := 2.5 * 1024 * 1024 // 2.5MB

		parts := splitToParts(context.Background(), totalParts, partSize, int(objectSize), source)

		var collectedParts []*PrepareDownloadPart
		for part := range parts {
			collectedParts = append(collectedParts, part)
		}

		assert.Len(t, collectedParts, 3)

		// First part: 0 to 1MB-1
		assert.Equal(t, int64(0), collectedParts[0].offset)
		assert.Equal(t, int64(1024*1024), collectedParts[0].size)
		assert.Equal(t, "bytes=0-1048575", collectedParts[0].byteRange)

		// Second part: 1MB to 2MB-1
		assert.Equal(t, int64(1024*1024), collectedParts[1].offset)
		assert.Equal(t, int64(1024*1024), collectedParts[1].size)
		assert.Equal(t, "bytes=1048576-2097151", collectedParts[1].byteRange)

		// Third part: 2MB to end
		assert.Equal(t, int64(2*1024*1024), collectedParts[2].offset)
		assert.Equal(t, int64(int(objectSize)-2*1024*1024), collectedParts[2].size)
		assert.Equal(t, "bytes=2097152-2621439", collectedParts[2].byteRange)
	})

	t.Run("Exact multiple of part size", func(t *testing.T) {
		totalParts := 2
		partSize := 1024 * 1024       // 1MB
		objectSize := 2 * 1024 * 1024 // Exactly 2MB

		parts := splitToParts(context.Background(), totalParts, partSize, objectSize, source)

		var collectedParts []*PrepareDownloadPart
		for part := range parts {
			collectedParts = append(collectedParts, part)
		}

		assert.Len(t, collectedParts, 2)

		// First part: 0 to 1MB-1
		assert.Equal(t, int64(0), collectedParts[0].offset)
		assert.Equal(t, int64(1024*1024), collectedParts[0].size)

		// Second part: 1MB to 2MB-1
		assert.Equal(t, int64(1024*1024), collectedParts[1].offset)
		assert.Equal(t, int64(1024*1024), collectedParts[1].size)
	})
}

func TestDownloadedPart(t *testing.T) {
	t.Run("Create DownloadedPart", func(t *testing.T) {
		// Create a temporary file for testing
		tempFile, err := os.CreateTemp("", "test_download_part_*.tmp")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		testContent := []byte("test content")
		_, err = tempFile.Write(testContent)
		require.NoError(t, err)
		tempFile.Close()

		part := &DownloadedPart{
			size:         1024,
			tempFilePath: tempFile.Name(),
			offset:       0,
			partNum:      1,
			err:          nil,
		}

		assert.Equal(t, int64(1024), part.size)
		assert.Equal(t, tempFile.Name(), part.tempFilePath)
		assert.Equal(t, int64(0), part.offset)
		assert.Equal(t, 1, part.partNum)
		assert.NoError(t, part.err)

		// Verify temp file contains expected content
		data, err := os.ReadFile(part.tempFilePath)
		require.NoError(t, err)
		assert.Equal(t, testContent, data)
	})

	t.Run("DownloadedPart with error", func(t *testing.T) {
		part := &DownloadedPart{
			size:         0,
			tempFilePath: "",
			offset:       0,
			partNum:      1,
			err:          assert.AnError,
		}

		assert.Equal(t, int64(0), part.size)
		assert.Equal(t, "", part.tempFilePath)
		assert.Error(t, part.err)
	})
}

func TestPrepareDownloadPart(t *testing.T) {
	t.Run("Create PrepareDownloadPart", func(t *testing.T) {
		part := &PrepareDownloadPart{
			namespace: "test-namespace",
			bucket:    "test-bucket",
			object:    "test-object.txt",
			byteRange: "bytes=0-1023",
			offset:    0,
			partNum:   1,
			size:      1024,
		}

		assert.Equal(t, "test-namespace", part.namespace)
		assert.Equal(t, "test-bucket", part.bucket)
		assert.Equal(t, "test-object.txt", part.object)
		assert.Equal(t, "bytes=0-1023", part.byteRange)
		assert.Equal(t, int64(0), part.offset)
		assert.Equal(t, 1, part.partNum)
		assert.Equal(t, int64(1024), part.size)
	})
}

func TestFileToDownload(t *testing.T) {
	t.Run("Create FileToDownload", func(t *testing.T) {
		source := ObjectURI{
			Namespace:  "test-namespace",
			BucketName: "test-bucket",
			ObjectName: "test-object.txt",
		}

		file := &FileToDownload{
			source:         source,
			targetFilePath: "/local/path/test-object.txt",
		}

		assert.Equal(t, source, file.source)
		assert.Equal(t, "/local/path/test-object.txt", file.targetFilePath)
	})
}

func TestDownloadedFile(t *testing.T) {
	t.Run("Create DownloadedFile success", func(t *testing.T) {
		source := ObjectURI{
			Namespace:  "test-namespace",
			BucketName: "test-bucket",
			ObjectName: "test-object.txt",
		}

		file := &DownloadedFile{
			source:         source,
			targetFilePath: "/local/path/test-object.txt",
			Err:            nil,
		}

		assert.Equal(t, source, file.source)
		assert.Equal(t, "/local/path/test-object.txt", file.targetFilePath)
		assert.NoError(t, file.Err)
	})

	t.Run("Create DownloadedFile with error", func(t *testing.T) {
		source := ObjectURI{
			Namespace:  "test-namespace",
			BucketName: "test-bucket",
			ObjectName: "test-object.txt",
		}

		file := &DownloadedFile{
			source:         source,
			targetFilePath: "/local/path/test-object.txt",
			Err:            assert.AnError,
		}

		assert.Equal(t, source, file.source)
		assert.Equal(t, "/local/path/test-object.txt", file.targetFilePath)
		assert.Error(t, file.Err)
	})
}

func TestChunkUnit(t *testing.T) {
	t.Run("MB constant", func(t *testing.T) {
		assert.Equal(t, ChunkUnit(1000000), MB)
	})

	t.Run("maxPartRetries constant", func(t *testing.T) {
		assert.Equal(t, 3, maxPartRetries)
	})
}

// Test edge cases for part calculation
func TestPartCalculationEdgeCases(t *testing.T) {
	source := ObjectURI{
		Namespace:  "test-namespace",
		BucketName: "test-bucket",
		ObjectName: "test-object.txt",
	}

	t.Run("Zero parts", func(t *testing.T) {
		totalParts := 0
		partSize := 1024
		objectSize := 0

		parts := splitToParts(context.Background(), totalParts, partSize, objectSize, source)

		var collectedParts []*PrepareDownloadPart
		for part := range parts {
			collectedParts = append(collectedParts, part)
		}

		assert.Len(t, collectedParts, 0)
	})

	t.Run("Single byte file", func(t *testing.T) {
		totalParts := 1
		partSize := 1024
		objectSize := 1

		parts := splitToParts(context.Background(), totalParts, partSize, objectSize, source)

		var collectedParts []*PrepareDownloadPart
		for part := range parts {
			collectedParts = append(collectedParts, part)
		}

		assert.Len(t, collectedParts, 1)
		assert.Equal(t, int64(0), collectedParts[0].offset)
		assert.Equal(t, int64(1), collectedParts[0].size)
		assert.Equal(t, "bytes=0-0", collectedParts[0].byteRange)
	})

	t.Run("Large number of parts", func(t *testing.T) {
		totalParts := 1000
		partSize := 1024          // 1KB parts
		objectSize := 1000 * 1024 // 1000KB total

		parts := splitToParts(context.Background(), totalParts, partSize, objectSize, source)

		var collectedParts []*PrepareDownloadPart
		for part := range parts {
			collectedParts = append(collectedParts, part)
		}

		assert.Len(t, collectedParts, 1000)

		// Check first part
		assert.Equal(t, int64(0), collectedParts[0].offset)
		assert.Equal(t, int64(1024), collectedParts[0].size)
		assert.Equal(t, 0, collectedParts[0].partNum)

		// Check last part
		lastPart := collectedParts[999]
		assert.Equal(t, int64(999*1024), lastPart.offset)
		assert.Equal(t, int64(1024), lastPart.size)
		assert.Equal(t, 999, lastPart.partNum)
	})
}

// Test byte range calculations
func TestByteRangeCalculations(t *testing.T) {
	source := ObjectURI{
		Namespace:  "test-namespace",
		BucketName: "test-bucket",
		ObjectName: "test-object.txt",
	}

	tests := []struct {
		name       string
		totalParts int
		partSize   int
		objectSize int
		expected   []string
	}{
		{
			name:       "Simple 2-part split",
			totalParts: 2,
			partSize:   100,
			objectSize: 200,
			expected:   []string{"bytes=0-99", "bytes=100-199"},
		},
		{
			name:       "3-part split with remainder",
			totalParts: 3,
			partSize:   100,
			objectSize: 250,
			expected:   []string{"bytes=0-99", "bytes=100-199", "bytes=200-249"},
		},
		{
			name:       "Single part",
			totalParts: 1,
			partSize:   1000,
			objectSize: 500,
			expected:   []string{"bytes=0-499"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts := splitToParts(context.Background(), tt.totalParts, tt.partSize, tt.objectSize, source)

			var collectedParts []*PrepareDownloadPart
			for part := range parts {
				collectedParts = append(collectedParts, part)
			}

			assert.Len(t, collectedParts, len(tt.expected))
			for i, expectedRange := range tt.expected {
				assert.Equal(t, expectedRange, collectedParts[i].byteRange,
					"Part %d byte range mismatch", i)
			}
		})
	}
}
