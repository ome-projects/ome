package ociobjectstore

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultipartDownloadClosesPartFilesDuringAssembly(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("file descriptor enumeration requires /dev/fd")
	}
	const objectName = "model.bin"
	const partSize = 1 << 20
	const partCount = 16
	payload := bytes.Repeat([]byte{'a'}, partSize)
	targetDir := t.TempDir()
	partTempDir := t.TempDir()
	t.Setenv("TMPDIR", partTempDir)
	readOpenFileNames := func() ([]string, error) {
		dir, err := os.Open("/dev/fd")
		if err != nil {
			return nil, err
		}
		defer dir.Close()
		// Read only names: statting special descriptors can return EBADF on macOS.
		return dir.Readdirnames(-1)
	}
	initialFiles, err := readOpenFileNames()
	require.NoError(t, err)
	maxOpenFiles := len(initialFiles)
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Range") == "" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":%d}]}`, objectName, partSize*partCount))),
				Request:    request,
			}, nil
		}
		files, err := readOpenFileNames()
		if err != nil {
			return nil, err
		}
		maxOpenFiles = max(maxOpenFiles, len(files))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewReader(payload)),
			Request:    request,
		}, nil
	})
	store.logger = &MockTestLogger{}
	err = store.MultipartDownload(ObjectURI{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}, targetDir,
		WithChunkSize(1), WithThreads(1))
	require.NoError(t, err)
	// Allow the assembly file, current part, and runtime descriptors, but not
	// a descriptor retained for every already-assembled part.
	require.LessOrEqual(t, maxOpenFiles-len(initialFiles), 6)
	info, err := os.Stat(filepath.Join(targetDir, objectName))
	require.NoError(t, err)
	require.Equal(t, int64(partSize*partCount), info.Size())
	leftoverParts, err := os.ReadDir(partTempDir)
	require.NoError(t, err)
	require.Empty(t, leftoverParts)
}
