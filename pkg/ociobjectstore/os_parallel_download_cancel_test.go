package ociobjectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/stretchr/testify/require"
)

func TestBulkDownloadContextStopsMultipartAfterActiveRequest(t *testing.T) {
	const objectName = "model.safetensors"
	const partSize = 1 << 20
	partStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var partRequests atomic.Int32

	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Range") == "" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":%d}]}`, objectName, 2*partSize))),
				Request:    request,
			}, nil
		}
		if partRequests.Add(1) == 1 {
			close(partStarted)
		}
		select {
		case <-request.Context().Done():
			return nil, request.Context().Err()
		case <-release:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Length": []string{fmt.Sprint(partSize)}},
				Body:       io.NopCloser(bytes.NewReader(make([]byte, partSize))),
				Request:    request,
			}, nil
		}
	})
	store.logger = &MockTestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targetDir := t.TempDir()
	finished := make(chan error, 1)
	go func() {
		finished <- store.BulkDownloadContext(ctx, []ObjectURI{{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}}, targetDir, 1,
			WithChunkSize(1), WithThreads(1), WithForceMultipart(true), WithOverrideEnabled(true))
	}()

	select {
	case <-partStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("multipart download did not start")
	}
	cancel()
	select {
	case err := <-finished:
		t.Fatalf("multipart returned before its active request finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("multipart did not exit after its active request finished")
	}
	require.Equal(t, int32(1), partRequests.Load())
	_, err := os.Stat(filepath.Join(targetDir, objectName+".temp"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

type closeUnblocksReadCloser struct {
	readStarted chan struct{}
	closed      chan struct{}
	readOnce    sync.Once
	closeOnce   sync.Once
}

func (body *closeUnblocksReadCloser) Read([]byte) (int, error) {
	body.readOnce.Do(func() { close(body.readStarted) })
	<-body.closed
	return 0, io.ErrClosedPipe
}

func (body *closeUnblocksReadCloser) Close() error {
	body.closeOnce.Do(func() { close(body.closed) })
	return nil
}

func TestBulkDownloadContextCancelsActiveMultipartBody(t *testing.T) {
	const objectName = "model.safetensors"
	body := &closeUnblocksReadCloser{readStarted: make(chan struct{}), closed: make(chan struct{})}
	defer body.Close()
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Range") == "" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":%d}]}`, objectName, 1<<20))),
				Request:    request,
			}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body, Request: request}, nil
	})
	store.logger = &MockTestLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targetDir := t.TempDir()
	finished := make(chan error, 1)
	go func() {
		finished <- store.BulkDownloadContext(ctx, []ObjectURI{{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}}, targetDir, 1,
			WithChunkSize(1), WithThreads(1), WithForceMultipart(true), WithOverrideEnabled(true))
	}()

	select {
	case <-body.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("multipart body was not read")
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(500 * time.Millisecond):
		body.Close()
		<-finished
		t.Fatal("cancel did not close the active multipart response body")
	}
}

func TestMultipartPartFailureStopsSibling(t *testing.T) {
	const objectName = "model.safetensors"
	const partSize = 1 << 20
	partTempDir := t.TempDir()
	t.Setenv("TMPDIR", partTempDir)
	siblingBody := &closeUnblocksReadCloser{readStarted: make(chan struct{}), closed: make(chan struct{})}
	defer siblingBody.Close()
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		switch request.Header.Get("Range") {
		case "":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":%d}]}`, objectName, 2*partSize))),
				Request:    request,
			}, nil
		case "bytes=0-1048575":
			<-siblingBody.readStarted
			return objectStorageTestResponse(request, http.StatusBadRequest, http.Header{}), nil
		default:
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: siblingBody, Request: request}, nil
		}
	})
	store.logger = &MockTestLogger{}
	targetDir := t.TempDir()
	err := store.MultipartDownload(ObjectURI{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}, targetDir,
		WithChunkSize(1), WithThreads(2))
	require.ErrorContains(t, err, "error downloading part")
	select {
	case <-siblingBody.closed:
	default:
		t.Fatal("multipart returned while a sibling part was still reading")
	}
	partFiles, err := filepath.Glob(filepath.Join(partTempDir, "ome_download_part_*.tmp"))
	require.NoError(t, err)
	require.Empty(t, partFiles)
	_, err = os.Stat(filepath.Join(targetDir, objectName+".temp"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMultipartDownloadContextCompletesFile(t *testing.T) {
	const objectName = "model.safetensors"
	const partSize = 1 << 20
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Range") == "" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":%d}]}`, objectName, 2*partSize))),
				Request:    request,
			}, nil
		}
		value := byte('a')
		if request.Header.Get("Range") == "bytes=1048576-2097151" {
			value = 'b'
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{value}, partSize))),
			Request:    request,
		}, nil
	})
	store.logger = &MockTestLogger{}
	targetDir := t.TempDir()
	err := store.MultipartDownloadContext(context.Background(),
		ObjectURI{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}, targetDir,
		WithChunkSize(1), WithThreads(2))
	require.NoError(t, err)
	content, err := os.ReadFile(filepath.Join(targetDir, objectName))
	require.NoError(t, err)
	require.Equal(t, append(bytes.Repeat([]byte{'a'}, partSize), bytes.Repeat([]byte{'b'}, partSize)...), content)
	_, err = os.Stat(filepath.Join(targetDir, objectName+".temp"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMultipartCancellationWaitsForSDKRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retryStarted := make(chan struct{})
	releaseRetry := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseRetry) })
	body := &closeUnblocksReadCloser{readStarted: make(chan struct{}), closed: make(chan struct{})}
	defer body.Close()
	var requests atomic.Int32
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if requests.Add(1) == 1 {
			cancel()
			return objectStorageTestResponse(request, http.StatusServiceUnavailable, http.Header{}), nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body, Request: request}, nil
	})
	store.logger = &MockTestLogger{}
	policy := common.DefaultRetryPolicyWithoutEventualConsistency()
	policy.NextDuration = func(common.OCIOperationResponse) time.Duration {
		close(retryStarted)
		<-releaseRetry
		return 0
	}
	store.Client.Configuration.RetryPolicy = &policy
	parts := make(chan *PrepareDownloadPart, 1)
	parts <- &PrepareDownloadPart{namespace: "ns", bucket: "bucket", object: "model.safetensors", byteRange: "bytes=0-1"}
	close(parts)
	results := make(chan *DownloadedPart, 1)
	finished := make(chan struct{})
	go func() {
		store.downloadFilePart(ctx, parts, results)
		close(finished)
	}()

	select {
	case <-retryStarted:
	case <-finished:
		t.Fatal("multipart returned without preserving the active SDK retry")
	case <-time.After(5 * time.Second):
		t.Fatal("SDK retry did not start")
	}
	select {
	case <-finished:
		t.Fatal("multipart returned before the active SDK retry finished")
	default:
	}
	releaseOnce.Do(func() { close(releaseRetry) })
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("multipart did not exit after the active SDK retry finished")
	}
	require.Equal(t, int32(2), requests.Load())
	require.Empty(t, results, "canceled part must not publish downloaded data")
	select {
	case <-body.closed:
	default:
		t.Fatal("response body returned after cancellation was not closed")
	}
	select {
	case <-body.readStarted:
		t.Fatal("response body returned after cancellation was read")
	default:
	}
}

func TestMultipartPartRetriesTransientFailure(t *testing.T) {
	var requests atomic.Int32
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if requests.Add(1) <= 3 {
			return objectStorageTestResponse(request, http.StatusServiceUnavailable, http.Header{}), nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader([]byte{'a'})), Request: request}, nil
	})
	store.logger = &MockTestLogger{}
	parts := make(chan *PrepareDownloadPart, 1)
	parts <- &PrepareDownloadPart{namespace: "ns", bucket: "bucket", object: "model.safetensors", byteRange: "bytes=0-0"}
	close(parts)
	results := make(chan *DownloadedPart, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.downloadFilePart(ctx, parts, results)
	part := <-results
	require.NoError(t, part.err)
	defer os.Remove(part.tempFilePath)
	require.Equal(t, int32(4), requests.Load())
}

func TestMultipartDownloadRetainsSDKRetryPolicy(t *testing.T) {
	const objectName = "model.safetensors"
	var retryHeader atomic.Value
	var partRequests atomic.Int32
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Range") == "" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":1}]}`, objectName))),
				Request:    request,
			}, nil
		}
		retryHeader.Store(request.Header.Get("opc-client-retries"))
		if partRequests.Add(1) == 1 {
			return objectStorageTestResponse(request, http.StatusServiceUnavailable, http.Header{}), nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader([]byte{'a'})), Request: request}, nil
	})
	store.logger = &MockTestLogger{}
	err := store.MultipartDownload(ObjectURI{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}, t.TempDir(),
		WithChunkSize(1), WithThreads(1))
	require.NoError(t, err)
	require.Equal(t, "true", retryHeader.Load())
	require.Equal(t, int32(2), partRequests.Load())
}

func TestMultipartEntrypointsRetainSDKRetryPolicy(t *testing.T) {
	const objectName = "model.safetensors"
	for _, test := range []struct {
		name string
		run  func(*OCIOSDataStore, ObjectURI, string) error
	}{
		{"strategy", func(store *OCIOSDataStore, object ObjectURI, target string) error {
			return store.DownloadWithStrategy(object, target, WithForceMultipart(true), WithOverrideEnabled(true), WithChunkSize(1), WithThreads(1))
		}},
		{"bulk", func(store *OCIOSDataStore, object ObjectURI, target string) error {
			return store.BulkDownload([]ObjectURI{object}, target, 1, WithForceMultipart(true), WithOverrideEnabled(true), WithChunkSize(1), WithThreads(1))
		}},
		{"cancelable bulk", func(store *OCIOSDataStore, object ObjectURI, target string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			return store.BulkDownloadContext(ctx, []ObjectURI{object}, target, 1, WithForceMultipart(true), WithOverrideEnabled(true), WithChunkSize(1), WithThreads(1))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var retryHeader atomic.Value
			var partRequests atomic.Int32
			store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
				if request.Header.Get("Range") == "" {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":1}]}`, objectName))),
						Request:    request,
					}, nil
				}
				retryHeader.Store(request.Header.Get("opc-client-retries"))
				if partRequests.Add(1) == 1 {
					return objectStorageTestResponse(request, http.StatusServiceUnavailable, http.Header{}), nil
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader([]byte{'a'})), Request: request}, nil
			})
			store.logger = &MockTestLogger{}
			err := test.run(store, ObjectURI{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}, t.TempDir())
			require.NoError(t, err)
			require.Equal(t, "true", retryHeader.Load())
			require.Equal(t, int32(2), partRequests.Load())
		})
	}
}

func TestBulkDownloadContextDoesNotStartStandardAfterCancellation(t *testing.T) {
	const objectName = "small.bin"
	listStarted := make(chan struct{})
	releaseList := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseList) })
	var getRequests atomic.Int32
	store := newTestOCIOSDataStore(func(request *http.Request) (*http.Response, error) {
		if request.URL.Query().Get("fields") != "" {
			close(listStarted)
			<-releaseList
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"objects":[{"name":%q,"size":1}]}`, objectName))),
				Request:    request,
			}, nil
		}
		getRequests.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader([]byte{'a'})), Request: request}, nil
	})
	store.logger = &MockTestLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targetDir := t.TempDir()
	finished := make(chan error, 1)
	go func() {
		finished <- store.BulkDownloadContext(ctx, []ObjectURI{{Namespace: "ns", BucketName: "bucket", ObjectName: objectName}}, targetDir, 1,
			WithOverrideEnabled(true))
	}()
	select {
	case <-listStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("object listing did not start")
	}
	cancel()
	releaseOnce.Do(func() { close(releaseList) })
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("bulk download did not return after listing")
	}
	require.Zero(t, getRequests.Load())
}
