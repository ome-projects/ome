package replicator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/xet"

	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

type downloadProgressLogger struct {
	logging.Interface
	lines []string
}

func (l *downloadProgressLogger) Infof(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func TestDownloadSnapshotWithTimeouts_ProgressLogs(t *testing.T) {
	for _, staleTimeout := range []time.Duration{0, 5 * time.Minute} {
		t.Run(staleTimeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger := &downloadProgressLogger{Interface: testingPkg.SetupMockLogger()}
				hub := &fakeHubClient{}
				hub.download = func(context.Context, *xet.SnapshotRequest) (string, error) {
					if hub.progressHandler == nil {
						return "", fmt.Errorf("progress reporting must work with watchdog disabled")
					}
					time.Sleep(time.Second)
					update := xet.ProgressUpdate{CompletedBytes: 100, TotalBytes: 1000,
						CurrentFile: "https://example.invalid/weights?token=secret"}
					hub.progressHandler(update)
					time.Sleep(30 * time.Second)
					update.CompletedBytes = 200
					hub.progressHandler(update)
					// Repeated callbacks without increased counters are not progress.
					for i := 0; i < 69; i++ {
						time.Sleep(time.Second)
						hub.progressHandler(update)
					}
					return "/tmp/model", nil
				}
				_, err := downloadSnapshotWithTimeouts(hub, &xet.SnapshotRequest{}, hfDownloadOptions{
					StaleProgressTimeout: staleTimeout,
				}, logger)
				if err != nil {
					t.Fatal(err)
				}
				var progress []string
				for _, line := range logger.lines {
					if strings.Contains(line, "download progress:") {
						progress = append(progress, line)
					}
					if strings.Contains(line, "secret") || strings.Contains(line, "example.invalid") {
						t.Fatalf("sensitive progress metadata logged: %s", line)
					}
				}
				if len(progress) != 3 {
					t.Fatalf("expected three bounded periodic logs, got %v", progress)
				}
				for i, want := range []string{
					"downloaded_bytes=100 total_bytes=1000 rate_bytes_per_second=3.33 last_progress_age=29s",
					"downloaded_bytes=200 total_bytes=1000 rate_bytes_per_second=3.33 last_progress_age=29s",
					"downloaded_bytes=200 total_bytes=1000 rate_bytes_per_second=0.00 last_progress_age=59s",
				} {
					if !strings.Contains(progress[i], want) {
						t.Errorf("log %d: got %q, want %q", i, progress[i], want)
					}
				}
			})
		})
	}
}

// A failed callback registration must not prevent the download or imply a stall.
type unavailableProgressHub struct {
	*fakeHubClient
}

func (h *unavailableProgressHub) SetProgressHandler(xet.ProgressHandler, time.Duration) error {
	return fmt.Errorf("progress not supported")
}

func TestDownloadSnapshotWithTimeouts_ProgressUnavailable(t *testing.T) {
	for _, tt := range []struct {
		name              string
		update            *xet.ProgressUpdate
		registrationFails bool
		want              string
	}{
		{name: "no callbacks", want: "download progress: unavailable"},
		{name: "registration failure", registrationFails: true, want: "download progress: unavailable"},
		{name: "unknown total", update: &xet.ProgressUpdate{CompletedBytes: 100},
			want: "downloaded_bytes=100 total_bytes=unavailable rate_bytes_per_second=3.33 last_progress_age=29s"},
		{name: "no observed progress", update: &xet.ProgressUpdate{TotalBytes: 1000},
			want: "downloaded_bytes=0 total_bytes=1000 rate_bytes_per_second=0.00 last_progress_age=unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger := &downloadProgressLogger{Interface: testingPkg.SetupMockLogger()}
				hub := &fakeHubClient{}
				hub.download = func(context.Context, *xet.SnapshotRequest) (string, error) {
					time.Sleep(time.Second)
					if tt.update != nil {
						hub.progressHandler(*tt.update)
					}
					time.Sleep(30 * time.Second)
					return "/tmp/model", nil
				}
				var client HubClient = hub
				if tt.registrationFails {
					client = &unavailableProgressHub{fakeHubClient: hub}
				}
				_, err := downloadSnapshotWithTimeouts(client, &xet.SnapshotRequest{}, hfDownloadOptions{}, logger)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(strings.Join(logger.lines, "\n"), tt.want) {
					t.Fatalf("missing %q in logs: %v", tt.want, logger.lines)
				}
				count := len(logger.lines)
				time.Sleep(time.Minute)
				if len(logger.lines) != count {
					t.Fatal("progress logging continued after completion")
				}
			})
		})
	}
}

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
