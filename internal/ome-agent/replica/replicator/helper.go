package replicator

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"sigs.k8s.io/ome/pkg/xet"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

const (
	DefaultUploadChunkSizeInMB   = 50
	DefaultUploadThreads         = 10
	DefaultDownloadChunkSizeInMB = 20
	DefaultDownloadThreads       = 20

	ReplicaWorkspacePath = "replica"

	MD5ChecksumAlgorithm       = "md5"
	SHA256ChecksumAlgorithm    = "sha256"
	OCIObjectMD5MetadataKey    = "opc-meta-md5"
	OCIObjectSHA256MetadataKey = "opc-meta-sha256"
)

var (
	downloadSnapHook = func(ctx context.Context, c *xet.Client, req *xet.SnapshotRequest) (string, error) {
		return c.DownloadSnapshotWithContext(ctx, req)
	}
	setProgressHandlerHook = func(c *xet.Client, handler xet.ProgressHandler, throttle time.Duration) error {
		return c.SetProgressHandler(handler, throttle)
	}
	disableProgressHook                   = func(c *xet.Client) error { return c.DisableProgress() }
	downloadFromHFFunc                    = downloadFromHF
	downloadObjectFunc                    = DownloadObject
	uploadDirectoryToOCIOSDataStoreFunc   = uploadDirectoryToOCIOSDataStore
	uploadObjectToOCIOSDataStoreFunc      = UploadObjectToOCIOSDataStore
	getObjectMetadataWithChecksumFunc     = GetObjectMetadatWithFileChecksum
	downloadObjectsFromOCIOSDataStoreFunc = downloadObjectsFromOCIOSDataStore
)

type hfDownloadOptions struct {
	DownloadTimeout              time.Duration
	StaleProgressTimeout         time.Duration
	ProgressCallbackThrottleTime time.Duration
}

type hfSnapshotDownloadResult struct {
	path string
	err  error
}

type hfDownloadProgressSnapshot struct {
	completedBytes            uint64
	completedFiles            uint32
	currentFileCompletedBytes uint64
}

func downloadSnapshotWithTimeouts(
	hubClient *xet.Client,
	req *xet.SnapshotRequest,
	opts hfDownloadOptions,
	logger logging.Interface,
) (string, error) {
	ctx := context.Background()
	cancel := func() {}
	if opts.DownloadTimeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, opts.DownloadTimeout)
		cancel = timeoutCancel
	}
	defer cancel()

	progressed := make(chan struct{}, 1)
	staleTimedOut := make(chan struct{})
	var closeStaleOnce sync.Once
	var lastProgress hfDownloadProgressSnapshot
	var lastProgressMu sync.Mutex
	progressHandlerInstalled := false
	downloadFinished := false

	if opts.StaleProgressTimeout > 0 {
		if opts.ProgressCallbackThrottleTime <= 0 {
			opts.ProgressCallbackThrottleTime = 250 * time.Millisecond
		}
		if err := setProgressHandlerHook(hubClient, func(update xet.ProgressUpdate) {
			current := hfDownloadProgressSnapshot{
				completedBytes:            update.CompletedBytes,
				completedFiles:            update.CompletedFiles,
				currentFileCompletedBytes: update.CurrentFileCompletedBytes,
			}
			lastProgressMu.Lock()
			defer lastProgressMu.Unlock()
			if current == lastProgress {
				return
			}
			lastProgress = current
			select {
			case progressed <- struct{}{}:
			default:
			}
		}, opts.ProgressCallbackThrottleTime); err != nil {
			logger.Warnf("Unable to install HuggingFace/Xet progress watchdog: %v", err)
		} else {
			progressHandlerInstalled = true
			// Only disable the progress handler once the download goroutine has
			// actually returned (downloadFinished). On the timeout/cancellation path
			// the goroutine is still running inside DownloadSnapshotWithContext and the
			// native side may invoke the progress callback concurrently; disabling it
			// here would race with that in-flight callback. The handler is left in place
			// and torn down when the process exits shortly after the non-zero return.
			defer func() {
				if !downloadFinished {
					return
				}
				if err := disableProgressHook(hubClient); err != nil {
					logger.Warnf("Unable to disable HuggingFace/Xet progress watchdog: %v", err)
				}
			}()
		}
	}

	if progressHandlerInstalled {
		staleCtx, staleCancel := context.WithCancel(ctx)
		ctx = staleCtx
		defer staleCancel()

		go func() {
			timer := time.NewTimer(opts.StaleProgressTimeout)
			defer timer.Stop()
			for {
				select {
				case <-staleCtx.Done():
					return
				case <-progressed:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(opts.StaleProgressTimeout)
				case <-timer.C:
					closeStaleOnce.Do(func() { close(staleTimedOut) })
					staleCancel()
					return
				}
			}
		}()
	}

	start := time.Now()
	logger.Infof("Starting HuggingFace/Xet snapshot download for repo %s revision %s with timeout %v and stale-progress timeout %v",
		req.RepoID, req.Revision, opts.DownloadTimeout, opts.StaleProgressTimeout)

	resultCh := make(chan hfSnapshotDownloadResult, 1)
	// Read the hook before starting the goroutine: on a timeout this function
	// returns while the download may still be running, so a read inside the
	// goroutine could race with a test restoring the hook.
	download := downloadSnapHook
	go func() {
		path, err := download(ctx, hubClient, req)
		resultCh <- hfSnapshotDownloadResult{path: path, err: err}
	}()

	select {
	case result := <-resultCh:
		downloadFinished = true
		duration := time.Since(start)
		if result.err != nil {
			if isStaleProgressTimeout(staleTimedOut) {
				return "", fmt.Errorf("HuggingFace/Xet snapshot download made no bytes/files progress for %s after %s: %w", opts.StaleProgressTimeout, duration, result.err)
			}
			if ctx.Err() == context.DeadlineExceeded {
				return "", fmt.Errorf("HuggingFace/Xet snapshot download exceeded timeout %s after %s: %w", opts.DownloadTimeout, duration, result.err)
			}
			return "", result.err
		}
		logger.Infof("HuggingFace/Xet snapshot download completed in %v", duration)
		return result.path, nil
	case <-ctx.Done():
		duration := time.Since(start)
		if isStaleProgressTimeout(staleTimedOut) {
			return "", fmt.Errorf("HuggingFace/Xet snapshot download made no bytes/files progress for %s after %s", opts.StaleProgressTimeout, duration)
		}
		return "", fmt.Errorf("HuggingFace/Xet snapshot download exceeded timeout %s after %s", opts.DownloadTimeout, duration)
	}
}

func isStaleProgressTimeout(staleTimedOut <-chan struct{}) bool {
	select {
	case <-staleTimedOut:
		return true
	default:
		return false
	}
}

func UploadObjectToOCIOSDataStore(ociOSDataStore *ociobjectstore.OCIOSDataStore, object ociobjectstore.ObjectURI, filePath string) error {
	if ociOSDataStore == nil {
		return fmt.Errorf("target ociOSDataStore is nil")
	}

	err := ociOSDataStore.MultipartFileUpload(filePath, object, DefaultUploadChunkSizeInMB, DefaultUploadThreads)
	if err != nil {
		ociOSDataStore.Config.AnotherLogger.Errorf("Failed to upload %s: %+v", object.ObjectName, err)
		return err
	}
	return nil
}

func DownloadObject(ociOSDataStore *ociobjectstore.OCIOSDataStore, srcObj ociobjectstore.ObjectURI, downloadPath string) error {
	if ociOSDataStore == nil {
		return fmt.Errorf("source ociOSDataStore is nil")
	}

	err := ociOSDataStore.MultipartDownload(srcObj, downloadPath,
		ociobjectstore.WithChunkSize(DefaultDownloadChunkSizeInMB),
		ociobjectstore.WithThreads(DefaultDownloadThreads))
	if err != nil {
		ociOSDataStore.Config.AnotherLogger.Errorf("Failed to download object %s: %+v", srcObj.ObjectName, err)
		return err
	}
	return nil
}

func PrepareObjectChannel(objects []common.ReplicationObject) chan common.ReplicationObject {
	objChan := make(chan common.ReplicationObject, len(objects))
	go func() {
		defer close(objChan)
		for _, object := range objects {
			objChan <- object
		}
	}()
	return objChan
}

func LogProgress(successCount, errorCount, totalObjects int, startTime time.Time, logger logging.Interface) {
	progress := float64(successCount+errorCount) / float64(totalObjects) * 100
	elapsedTime := time.Since(startTime)
	logger.Infof("Progress: %.2f%%, Success: %d, Errors: %d, Total: %d, Elapsed Time: %v", progress, successCount, errorCount, totalObjects, elapsedTime)
}

func GetFileChecksum(filePath string, algorithm string) (string, error) {
	var h hash.Hash

	switch algorithm {
	case MD5ChecksumAlgorithm:
		h = md5.New()
	case SHA256ChecksumAlgorithm:
		h = sha256.New()
	default:
		return "", fmt.Errorf("unsupported checksum algorithm: %s", algorithm)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func GetObjectMetadatWithFileChecksum(config *common.ChecksumConfig, filePath string, logger logging.Interface) map[string]string {
	var metadata map[string]string = nil
	if config != nil && config.UploadEnabled {
		checksum, err := GetFileChecksum(filePath, config.ChecksumAlgorithm)
		if err != nil {
			logger.Warnf("Failed to compute checksum for %s: %+v", filePath, err)
		}

		if config.ChecksumAlgorithm == MD5ChecksumAlgorithm {
			metadata = map[string]string{
				OCIObjectMD5MetadataKey: checksum,
			}
		} else if config.ChecksumAlgorithm == SHA256ChecksumAlgorithm {
			metadata = map[string]string{
				OCIObjectSHA256MetadataKey: checksum,
			}
		}
	}
	return metadata
}

func newChecksumLimiter(config *common.ChecksumConfig) chan struct{} {
	return make(chan struct{}, effectiveChecksumConcurrency(config))
}

func getObjectMetadataWithChecksumLimit(
	config *common.ChecksumConfig,
	filePath string,
	logger logging.Interface,
	limiter chan struct{},
) map[string]string {
	if config == nil || !config.UploadEnabled {
		return nil
	}

	limiter <- struct{}{}
	defer func() { <-limiter }()
	return getObjectMetadataWithChecksumFunc(config, filePath, logger)
}

func effectiveChecksumConcurrency(config *common.ChecksumConfig) int {
	if config == nil || config.Concurrency < 1 {
		return 1
	}
	return config.Concurrency
}
