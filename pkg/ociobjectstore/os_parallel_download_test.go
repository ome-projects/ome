package ociobjectstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
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

		parts := splitToParts(totalParts, partSize, objectSize, source)

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

		parts := splitToParts(totalParts, partSize, int(objectSize), source)

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

		parts := splitToParts(totalParts, partSize, objectSize, source)

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
		part := &DownloadedPart{
			size:    1024,
			partNum: 1,
			err:     nil,
		}

		assert.Equal(t, int64(1024), part.size)
		assert.Equal(t, 1, part.partNum)
		assert.NoError(t, part.err)
	})

	t.Run("DownloadedPart with error", func(t *testing.T) {
		part := &DownloadedPart{
			size:    0,
			partNum: 1,
			err:     assert.AnError,
		}

		assert.Equal(t, int64(0), part.size)
		assert.Error(t, part.err)
	})
}

func TestValidateRangeResponse(t *testing.T) {
	part := &PrepareDownloadPart{
		byteRange:  "bytes=250-499",
		offset:     250,
		size:       250,
		objectSize: 1000,
	}
	validContentLength := int64(250)
	validContentRange := "bytes 250-499/1000"

	newResponse := func(statusCode int, contentLength *int64, contentRange *string) objectstorage.GetObjectResponse {
		return objectstorage.GetObjectResponse{
			RawResponse:   &http.Response{StatusCode: statusCode},
			Content:       io.NopCloser(bytes.NewReader(make([]byte, validContentLength))),
			ContentLength: contentLength,
			ContentRange:  contentRange,
		}
	}

	t.Run("accepts exact partial response", func(t *testing.T) {
		response := newResponse(http.StatusPartialContent, &validContentLength, &validContentRange)
		assert.NoError(t, validateRangeResponse(response, part))
	})

	t.Run("rejects full object response", func(t *testing.T) {
		fullObjectLength := int64(1000)
		response := newResponse(http.StatusOK, &fullObjectLength, nil)
		err := validateRangeResponse(response, part)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected HTTP 206")
	})

	t.Run("rejects wrong content range", func(t *testing.T) {
		wrongContentRange := "bytes 0-249/1000"
		response := newResponse(http.StatusPartialContent, &validContentLength, &wrongContentRange)
		err := validateRangeResponse(response, part)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected Content-Range")
	})

	t.Run("rejects wrong content length", func(t *testing.T) {
		wrongContentLength := int64(1000)
		response := newResponse(http.StatusPartialContent, &wrongContentLength, &validContentRange)
		err := validateRangeResponse(response, part)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected Content-Length 250")
	})

	t.Run("rejects missing response body", func(t *testing.T) {
		response := newResponse(http.StatusPartialContent, &validContentLength, &validContentRange)
		response.Content = nil
		err := validateRangeResponse(response, part)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing response body")
	})
}

func TestWritePartAtConcurrentOffsets(t *testing.T) {
	contents := [][]byte{
		bytes.Repeat([]byte("a"), 64*1024),
		bytes.Repeat([]byte("b"), 64*1024),
		bytes.Repeat([]byte("c"), 64*1024),
		bytes.Repeat([]byte("d"), 31*1024),
	}

	var totalSize int64
	parts := make([]*PrepareDownloadPart, 0, len(contents))
	for i, content := range contents {
		parts = append(parts, &PrepareDownloadPart{
			offset:  totalSize,
			partNum: i,
			size:    int64(len(content)),
		})
		totalSize += int64(len(content))
	}

	target, err := os.CreateTemp(t.TempDir(), "direct-write-*.temp")
	require.NoError(t, err)
	defer target.Close()
	_, err = preallocateFile(target, totalSize)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errorsByPart := make(chan error, len(parts))
	// Start in reverse order to prove correctness does not depend on a shared
	// file cursor or in-order part completion.
	for i := len(parts) - 1; i >= 0; i-- {
		part := parts[i]
		content := contents[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			written, writeErr := writePartAt(target, part, bytes.NewReader(content))
			if writeErr == nil && written != int64(len(content)) {
				writeErr = errors.New("unexpected written byte count")
			}
			errorsByPart <- writeErr
		}()
	}
	wg.Wait()
	close(errorsByPart)
	for writeErr := range errorsByPart {
		require.NoError(t, writeErr)
	}

	require.NoError(t, target.Sync())
	actual, err := os.ReadFile(target.Name())
	require.NoError(t, err)
	assert.Equal(t, bytes.Join(contents, nil), actual)
}

func TestWritePartAtRejectsShortResponse(t *testing.T) {
	target, err := os.CreateTemp(t.TempDir(), "short-response-*.temp")
	require.NoError(t, err)
	defer target.Close()
	_, err = preallocateFile(target, 8)
	require.NoError(t, err)

	part := &PrepareDownloadPart{offset: 0, partNum: 0, size: 8}
	written, err := writePartAt(target, part, bytes.NewReader([]byte("short")))

	assert.Equal(t, int64(5), written)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestWritePartAtDoesNotCrossPartBoundary(t *testing.T) {
	target, err := os.CreateTemp(t.TempDir(), "long-response-*.temp")
	require.NoError(t, err)
	defer target.Close()
	_, err = preallocateFile(target, 8)
	require.NoError(t, err)

	part := &PrepareDownloadPart{offset: 2, partNum: 0, size: 3}
	written, err := writePartAt(target, part, bytes.NewReader([]byte("toolong")))
	require.NoError(t, err)
	assert.Equal(t, int64(3), written)

	actual, err := os.ReadFile(target.Name())
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 't', 'o', 'o', 0, 0, 0}, actual)
}

func TestWritePartAtRetryOverwritesPartialRange(t *testing.T) {
	target, err := os.CreateTemp(t.TempDir(), "retry-*.temp")
	require.NoError(t, err)
	defer target.Close()
	_, err = preallocateFile(target, 8)
	require.NoError(t, err)

	part := &PrepareDownloadPart{offset: 0, partNum: 0, size: 8}
	written, err := writePartAt(target, part, bytes.NewReader([]byte("bad")))
	assert.Equal(t, int64(3), written)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

	written, err = writePartAt(target, part, bytes.NewReader([]byte("complete")))
	require.NoError(t, err)
	assert.Equal(t, int64(8), written)

	actual, err := os.ReadFile(target.Name())
	require.NoError(t, err)
	assert.Equal(t, []byte("complete"), actual)
}

func TestCopyCoalescedCombinesShortReadsIntoOneMiBWrites(t *testing.T) {
	content := bytes.Repeat([]byte("x"), 2*1024*1024+512*1024)
	destination := &countingWriter{}
	written, err := copyCoalesced(destination, &fixedChunkReader{
		reader:    bytes.NewReader(content),
		chunkSize: 16 * 1024,
	}, make([]byte, 1024*1024))

	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), written)
	assert.Equal(t, 3, destination.calls)
	assert.Equal(t, content, destination.Bytes())
}

func TestWriteLimiterCapsConcurrentWrites(t *testing.T) {
	const (
		limit      = 4
		writeCount = 32
	)
	limiter, err := NewWriteLimiter(limit)
	require.NoError(t, err)
	destination := &concurrencyTrackingWriter{}
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < writeCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			writer := &modelFileWriter{writer: destination, limiter: limiter}
			_, _ = writer.Write([]byte("test"))
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, limit, limiter.Limit())
	assert.LessOrEqual(t, destination.maximum(), limit)
	assert.Greater(t, destination.maximum(), 1)
}

func TestNewWriteLimiterRejectsNonPositiveConcurrency(t *testing.T) {
	for _, concurrency := range []int{0, -1} {
		t.Run(fmt.Sprintf("concurrency_%d", concurrency), func(t *testing.T) {
			limiter, err := NewWriteLimiter(concurrency)

			require.Error(t, err)
			assert.Nil(t, limiter)
		})
	}
}

type countingWriter struct {
	bytes.Buffer
	calls int
}

func (w *countingWriter) Write(buffer []byte) (int, error) {
	w.calls++
	return w.Buffer.Write(buffer)
}

type fixedChunkReader struct {
	reader    io.Reader
	chunkSize int
}

func (r *fixedChunkReader) Read(buffer []byte) (int, error) {
	if len(buffer) > r.chunkSize {
		buffer = buffer[:r.chunkSize]
	}
	return r.reader.Read(buffer)
}

type concurrencyTrackingWriter struct {
	mu        sync.Mutex
	active    int
	maxActive int
}

func (writer *concurrencyTrackingWriter) Write(buffer []byte) (int, error) {
	writer.mu.Lock()
	writer.active++
	if writer.active > writer.maxActive {
		writer.maxActive = writer.active
	}
	writer.mu.Unlock()

	time.Sleep(10 * time.Millisecond)

	writer.mu.Lock()
	writer.active--
	writer.mu.Unlock()
	return len(buffer), nil
}

func (writer *concurrencyTrackingWriter) maximum() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.maxActive
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

		parts := splitToParts(totalParts, partSize, objectSize, source)

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

		parts := splitToParts(totalParts, partSize, objectSize, source)

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

		parts := splitToParts(totalParts, partSize, objectSize, source)

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
			parts := splitToParts(tt.totalParts, tt.partSize, tt.objectSize, source)

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
