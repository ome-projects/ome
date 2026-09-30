package ociobjectstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

func TestBulkDownloadContextStopsAfterActiveFile(t *testing.T) {
	store := &OCIOSDataStore{logger: testingPkg.SetupMockLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	finished := make(chan error, 1)
	var downloaded []string
	targetDir := t.TempDir()

	go func() {
		finished <- store.bulkDownload(ctx, []ObjectURI{
			{ObjectName: "first"},
			{ObjectName: "second"},
		}, targetDir, 1, func(object ObjectURI, _ string, _ ...DownloadOption) error {
			downloaded = append(downloaded, object.ObjectName)
			if object.ObjectName == "first" {
				close(started)
				<-release
			}
			return nil
		})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first file did not start")
	}
	cancel()
	select {
	case err := <-finished:
		t.Fatalf("bulk download returned before its active file finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("bulk download did not stop after its active file finished")
	}
	require.Equal(t, []string{"first"}, downloaded)
}

func TestBulkDownloadContextStopsRetry(t *testing.T) {
	store := &OCIOSDataStore{logger: testingPkg.SetupMockLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0

	err := store.bulkDownload(ctx, []ObjectURI{{ObjectName: "first"}}, t.TempDir(), 1,
		func(ObjectURI, string, ...DownloadOption) error {
			attempts++
			cancel()
			return errors.New("download failed")
		})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

func TestBulkDownloadContextProcessesFiles(t *testing.T) {
	store := &OCIOSDataStore{logger: testingPkg.SetupMockLogger()}
	var downloaded []string
	err := store.bulkDownload(context.Background(), []ObjectURI{
		{ObjectName: "first"},
		{ObjectName: "second"},
	}, t.TempDir(), 1, func(object ObjectURI, _ string, _ ...DownloadOption) error {
		downloaded = append(downloaded, object.ObjectName)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, downloaded)
}

func TestBulkDownloadLegacyEmptyList(t *testing.T) {
	store := &OCIOSDataStore{logger: testingPkg.SetupMockLogger()}
	require.NoError(t, store.BulkDownload(nil, t.TempDir(), 1))
}
