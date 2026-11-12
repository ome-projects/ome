package replicator

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/xet"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

// fakeHubClient stands in for the HuggingFace client: the snapshot download is scripted, and
// the progress handler is kept so a scripted download can report progress.
type fakeHubClient struct {
	download        func(ctx context.Context, req *xet.SnapshotRequest) (string, error)
	progressHandler xet.ProgressHandler
	downloads       int
}

func (f *fakeHubClient) DownloadSnapshotWithContext(ctx context.Context, req *xet.SnapshotRequest) (string, error) {
	f.downloads++
	return f.download(ctx, req)
}

func (f *fakeHubClient) SetProgressHandler(handler xet.ProgressHandler, _ time.Duration) error {
	f.progressHandler = handler
	return nil
}

func (f *fakeHubClient) DisableProgress() error {
	return nil
}

// fakeTransfer records the objects downloaded and uploaded, and fails the ones a test names.
type fakeTransfer struct {
	mu          sync.Mutex
	downloads   []string
	uploads     []string
	downloadErr func(object ociobjectstore.ObjectURI) error
	uploadErr   func(object ociobjectstore.ObjectURI) error
}

func (f *fakeTransfer) Download(_ *ociobjectstore.OCIOSDataStore, object ociobjectstore.ObjectURI, _ string) error {
	f.mu.Lock()
	f.downloads = append(f.downloads, object.ObjectName)
	f.mu.Unlock()
	if f.downloadErr != nil {
		return f.downloadErr(object)
	}
	return nil
}

func (f *fakeTransfer) Upload(_ *ociobjectstore.OCIOSDataStore, object ociobjectstore.ObjectURI, _ string) error {
	f.mu.Lock()
	f.uploads = append(f.uploads, object.ObjectName)
	f.mu.Unlock()
	if f.uploadErr != nil {
		return f.uploadErr(object)
	}
	return nil
}

func (f *fakeTransfer) downloaded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.downloads...)
}

func (f *fakeTransfer) uploaded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.uploads...)
}

// failObject fails the named object and no other.
func failObject(name string, err error) func(ociobjectstore.ObjectURI) error {
	return func(object ociobjectstore.ObjectURI) error {
		if object.ObjectName == name {
			return err
		}
		return nil
	}
}

// failEvery fails every object.
func failEvery(err error) func(ociobjectstore.ObjectURI) error {
	return func(ociobjectstore.ObjectURI) error {
		return err
	}
}

// storeWithLogger is an object store that can log but has no client; the fake transfer never
// reaches the client.
func storeWithLogger(logger logging.Interface) *ociobjectstore.OCIOSDataStore {
	return &ociobjectstore.OCIOSDataStore{Config: &ociobjectstore.Config{AnotherLogger: logger}}
}

// writeFiles creates each named file under dir, with the name as its content.
func writeFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(name), 0o644))
	}
}
