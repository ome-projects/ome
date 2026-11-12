package replicator

import (
	"context"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/xet"

	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

// waitForCancel is a scripted snapshot download that only returns when its context ends.
func waitForCancel(ctx context.Context, _ *xet.SnapshotRequest) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

func TestDownloadSnapshotWithTimeouts_EndToEndTimeout(t *testing.T) {
	hub := &fakeHubClient{download: waitForCancel}

	_, err := downloadSnapshotWithTimeouts(hub, &xet.SnapshotRequest{
		RepoID:   "org/model",
		Revision: "main",
	}, hfDownloadOptions{
		DownloadTimeout: 10 * time.Millisecond,
	}, testingPkg.SetupMockLogger())

	if err == nil || !strings.Contains(err.Error(), "exceeded timeout") {
		t.Fatalf("expected end-to-end timeout, got %v", err)
	}
}

func TestDownloadSnapshotWithTimeouts_StaleProgressTimeout(t *testing.T) {
	hub := &fakeHubClient{download: waitForCancel}

	_, err := downloadSnapshotWithTimeouts(hub, &xet.SnapshotRequest{
		RepoID:   "org/model",
		Revision: "main",
	}, hfDownloadOptions{
		DownloadTimeout:      time.Second,
		StaleProgressTimeout: 10 * time.Millisecond,
	}, testingPkg.SetupMockLogger())

	if err == nil || !strings.Contains(err.Error(), "no bytes/files progress") {
		t.Fatalf("expected stale-progress timeout, got %v", err)
	}
}

func TestDownloadSnapshotWithTimeouts_ProgressPreventsStaleTimeout(t *testing.T) {
	hub := &fakeHubClient{}
	hub.download = func(ctx context.Context, _ *xet.SnapshotRequest) (string, error) {
		for i := uint64(1); i <= 5; i++ {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(15 * time.Millisecond):
				hub.progressHandler(xet.ProgressUpdate{CompletedBytes: i})
			}
		}
		return "/tmp/model", nil
	}

	path, err := downloadSnapshotWithTimeouts(hub, &xet.SnapshotRequest{
		RepoID:   "org/model",
		Revision: "main",
	}, hfDownloadOptions{
		DownloadTimeout:      time.Second,
		StaleProgressTimeout: 50 * time.Millisecond,
	}, testingPkg.SetupMockLogger())

	if err != nil {
		t.Fatalf("expected progress to prevent stale timeout, got %v", err)
	}
	if path != "/tmp/model" {
		t.Fatalf("unexpected path: got %s", path)
	}
}
