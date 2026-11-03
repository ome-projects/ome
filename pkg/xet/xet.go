package xet

/*
// macOS (darwin) – no -Bstatic/-Bdynamic
#cgo darwin LDFLAGS: -L${SRCDIR}/target/release -lxet -ldl -lm -lstdc++ -framework CoreFoundation -framework Security

// Linux – keep your existing static/dynamic behavior
#cgo linux LDFLAGS: -L${SRCDIR}/target/release -Wl,-Bstatic -lxet -Wl,-Bdynamic -lssl -lcrypto -ldl -lm -lstdc++
#include <stdlib.h>
#include "xet.h"
// Link-time version check
extern void xet_version_1_0_0(void);
static void (*xet_version_check)(void) = &xet_version_1_0_0;
// Go callback bridges
extern void goXetProgressCallback(XetProgressUpdate* update, void* user_data);
extern bool goXetShouldCancel(void* user_data);
*/
import "C"
import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/cgo"
	"time"
	"unsafe"
)

func init() {
	// Set default logging level if not already set
	if os.Getenv("RUST_LOG") == "" {
		// Default to warn level to reduce verbosity
		os.Setenv("RUST_LOG", "warn")
	}
}

//export goXetProgressCallback
func goXetProgressCallback(update *C.XetProgressUpdate, userData unsafe.Pointer) {
	if update == nil || userData == nil {
		return
	}

	handle := cgo.Handle(userData)
	value := handle.Value()
	handler, ok := value.(ProgressHandler)
	if !ok {
		return
	}

	progress := ProgressUpdate{
		Phase:                     ProgressPhase(int(update.phase)),
		TotalBytes:                uint64(update.total_bytes),
		CompletedBytes:            uint64(update.completed_bytes),
		TotalFiles:                uint32(update.total_files),
		CompletedFiles:            uint32(update.completed_files),
		CurrentFileCompletedBytes: uint64(update.current_file_completed_bytes),
		CurrentFileTotalBytes:     uint64(update.current_file_total_bytes),
	}

	if update.current_file != nil {
		progress.CurrentFile = C.GoString((*C.char)(update.current_file))
	}

	handler(progress)
}

//export goXetShouldCancel
func goXetShouldCancel(userData unsafe.Pointer) C.bool {
	if userData == nil {
		return C.bool(false)
	}

	handle := cgo.Handle(userData)
	value := handle.Value()
	bridge, ok := value.(*cancellationBridge)
	if !ok {
		return C.bool(false)
	}

	if bridge.ctx.Err() != nil {
		return C.bool(true)
	}

	return C.bool(false)
}

// SetLogLevel sets the logging level for the underlying Rust library
// Valid levels are: error, warn, info, debug, trace
func SetLogLevel(level string) {
	os.Setenv("RUST_LOG", level)
}

func (c *Client) SetProgressHandler(handler ProgressHandler, throttle time.Duration) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("client is not initialized")
	}

	if c.hasProgressCallback {
		C.xet_client_set_progress_callback(c.client, nil, nil, 0)
		c.progressHandle.Delete()
		c.hasProgressCallback = false
	}

	if handler == nil {
		return nil
	}

	if throttle <= 0 {
		throttle = 200 * time.Millisecond
	}

	handle := cgo.NewHandle(handler)
	throttleMs := C.uint32_t(throttle / time.Millisecond)
	errPtr := C.xet_client_set_progress_callback(
		c.client,
		(C.XetProgressCallback)(C.goXetProgressCallback),
		unsafe.Pointer(handle),
		throttleMs,
	)
	if errPtr != nil {
		handle.Delete()
		return convertError(errPtr)
	}

	c.progressHandle = handle
	c.hasProgressCallback = true
	return nil
}

func (c *Client) EnableConsoleProgress(label string, throttle time.Duration) error {
	return c.SetProgressHandler(func(update ProgressUpdate) {
		current := update.CurrentFile
		if current == "" {
			current = "-"
		}

		var pct float64
		if update.TotalBytes > 0 {
			pct = float64(update.CompletedBytes) * 100 / float64(update.TotalBytes)
		}

		fmt.Printf("\r[%s] %-11s %6.2f%% (%d/%d files) %s", label, update.Phase.String(), pct, update.CompletedFiles, update.TotalFiles, current)
		if update.Phase == ProgressPhaseFinalizing {
			fmt.Println()
		}
	}, throttle)
}

func (c *Client) DisableProgress() error {
	return c.SetProgressHandler(nil, 0)
}

// Client represents an xet-core client for HF Hub operations
type Client struct {
	client              *C.XetClient
	progressHandle      cgo.Handle
	hasProgressCallback bool
}

type ProgressPhase int

const (
	ProgressPhaseScanning ProgressPhase = iota
	ProgressPhaseDownloading
	ProgressPhaseFinalizing
)

func (p ProgressPhase) String() string {
	switch p {
	case ProgressPhaseScanning:
		return "scanning"
	case ProgressPhaseDownloading:
		return "downloading"
	case ProgressPhaseFinalizing:
		return "finalizing"
	default:
		return "unknown"
	}
}

// ProgressUpdate mirrors XetProgressUpdate in Rust.
type ProgressUpdate struct {
	Phase                     ProgressPhase
	TotalBytes                uint64
	CompletedBytes            uint64
	TotalFiles                uint32
	CompletedFiles            uint32
	CurrentFile               string
	CurrentFileCompletedBytes uint64
	CurrentFileTotalBytes     uint64
}

// ProgressHandler receives throttled progress updates from Rust.
type ProgressHandler func(ProgressUpdate)

type cancellationBridge struct {
	ctx context.Context
}

// DownloadRequest represents a file download request
type DownloadRequest struct {
	RepoID   string
	RepoType string
	Revision string
	Filename string
	LocalDir string
}

// SnapshotRequest represents a snapshot download request
type SnapshotRequest struct {
	RepoID         string
	RepoType       string
	Revision       string
	LocalDir       string
	AllowPatterns  []string
	IgnorePatterns []string
}

// FileInfo represents file information
type FileInfo struct {
	Path string
	Hash string
	Size uint64
}

// XetError represents an error from xet-core
type XetError struct {
	Code    int
	Message string
	Details string
}

func (e *XetError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("xet error %d: %s (%s)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("xet error %d: %s", e.Code, e.Message)
}

// NewClient creates a new xet client
func NewClient(config *Config) (*Client, error) {
	if config == nil {
		config = defaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	// Set log level if specified
	if config.LogLevel != "" {
		SetLogLevel(config.LogLevel)
	}

	var held cStrings
	defer held.free()
	cConfig := C.XetConfig{
		endpoint:                 held.add(config.Endpoint),
		token:                    held.add(config.Token),
		cache_dir:                held.add(config.CacheDir),
		max_concurrent_downloads: C.uint32_t(config.MaxConcurrentDownloads),
		enable_dedup:             C.bool(config.EnableDedup),
	}

	client := C.xet_client_new(&cConfig)
	if client == nil {
		return nil, fmt.Errorf("failed to create xet client")
	}

	return &Client{client: client}, nil
}

// Close releases the client resources
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	if c.hasProgressCallback {
		C.xet_client_set_progress_callback(c.client, nil, nil, 0)
		c.progressHandle.Delete()
		c.hasProgressCallback = false
	}

	if c.client != nil {
		C.xet_client_free(c.client)
		c.client = nil
	}
	return nil
}

// Ensure Client implements io.Closer
var _ io.Closer = (*Client)(nil)

// ListFiles lists files in a repository
func (c *Client) ListFiles(repoID string, revision string) ([]FileInfo, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client is closed")
	}

	cRepoID := C.CString(repoID)
	defer C.free(unsafe.Pointer(cRepoID))

	var cRevision *C.char
	if revision != "" {
		cRevision = C.CString(revision)
		defer C.free(unsafe.Pointer(cRevision))
	}

	var fileList *C.XetFileList
	err := C.xet_list_files(c.client, cRepoID, cRevision, &fileList)
	if err != nil {
		return nil, convertError(err)
	}
	defer C.xet_free_file_list(fileList)

	// Convert C file list to Go slice
	files := make([]FileInfo, fileList.count)
	for i := 0; i < int(fileList.count); i++ {
		cFile := (*C.XetFileInfo)(unsafe.Pointer(
			uintptr(unsafe.Pointer(fileList.files)) + uintptr(i)*unsafe.Sizeof(C.XetFileInfo{}),
		))
		files[i] = FileInfo{
			Path: C.GoString(cFile.path),
			Hash: C.GoString(cFile.hash),
			Size: uint64(cFile.size),
		}
	}

	return files, nil
}

// DownloadFile downloads a single file from a repository
func (c *Client) DownloadFile(req *DownloadRequest) (string, error) {
	return c.DownloadFileWithContext(context.Background(), req)
}

// DownloadFileWithContext downloads a file with context support
func (c *Client) DownloadFileWithContext(ctx context.Context, req *DownloadRequest) (string, error) {
	if c == nil || c.client == nil {
		return "", fmt.Errorf("client is closed")
	}

	cReq, free := convertDownloadRequest(req)
	defer free()
	token, release := cancellation(ctx)

	var outPath *C.char
	errPtr := C.xet_download_file(c.client, &cReq, token, &outPath)
	release()
	if errPtr != nil {
		return "", convertError(errPtr)
	}
	defer C.xet_free_string(outPath)
	return C.GoString(outPath), nil
}

// DownloadSnapshot downloads all files from a repository in parallel
func (c *Client) DownloadSnapshot(req *SnapshotRequest) (string, error) {
	return c.DownloadSnapshotWithContext(context.Background(), req)
}

// DownloadSnapshotWithContext downloads a snapshot with cancellation support
func (c *Client) DownloadSnapshotWithContext(ctx context.Context, req *SnapshotRequest) (string, error) {
	if c == nil || c.client == nil {
		return "", fmt.Errorf("client is closed")
	}
	if req == nil {
		return "", fmt.Errorf("snapshot request cannot be nil")
	}

	cReq, free := convertSnapshotRequest(req)
	defer free()
	token, release := cancellation(ctx)

	var outPath *C.char
	errPtr := C.xet_download_snapshot(
		c.client,
		cReq.repoID,
		cReq.repoType,
		cReq.revision,
		cReq.localDir,
		token,
		&outPath,
	)
	release()
	if errPtr != nil {
		return "", convertError(errPtr)
	}
	defer C.xet_free_string(outPath)
	return C.GoString(outPath), nil
}

// Helper functions

// cStrings holds the C copies of the Go strings one C call takes and frees
// them together once the call has returned. An empty Go string is passed as
// NULL, which the Rust side reads as "not given".
type cStrings struct {
	owned []*C.char
}

func (s *cStrings) add(value string) *C.char {
	if value == "" {
		return nil
	}
	p := C.CString(value)
	s.owned = append(s.owned, p)
	return p
}

func (s *cStrings) free() {
	for _, p := range s.owned {
		C.free(unsafe.Pointer(p))
	}
	s.owned = nil
}

// cancellation hands ctx to the Rust side as a token it polls while it works.
// A context that can never be cancelled passes no token. release drops the
// handle behind the token and is called once the C call has returned.
func cancellation(ctx context.Context) (token *C.XetCancellationToken, release func()) {
	if ctx == nil || ctx.Done() == nil {
		return nil, func() {}
	}
	handle := cgo.NewHandle(&cancellationBridge{ctx: ctx})
	token = &C.XetCancellationToken{
		callback:  (C.XetCancellationCallback)(C.goXetShouldCancel),
		user_data: unsafe.Pointer(handle),
	}
	return token, handle.Delete
}

// convertDownloadRequest is the C view of req; free releases its strings.
func convertDownloadRequest(req *DownloadRequest) (cReq C.XetDownloadRequest, free func()) {
	var held cStrings
	cReq = C.XetDownloadRequest{
		repo_id:   held.add(req.RepoID),
		repo_type: held.add(req.RepoType),
		revision:  held.add(req.Revision),
		filename:  held.add(req.Filename),
		local_dir: held.add(req.LocalDir),
	}
	return cReq, held.free
}

// snapshotRequest is the C view of a SnapshotRequest: the strings the Rust
// side takes one by one.
type snapshotRequest struct {
	repoID, repoType, revision, localDir *C.char
}

// convertSnapshotRequest is the C view of req; free releases its strings.
func convertSnapshotRequest(req *SnapshotRequest) (cReq snapshotRequest, free func()) {
	var held cStrings
	cReq = snapshotRequest{
		repoID:   held.add(req.RepoID),
		repoType: held.add(req.RepoType),
		revision: held.add(req.Revision),
		localDir: held.add(req.LocalDir),
	}
	return cReq, held.free
}

func convertError(err *C.XetError) error {
	if err == nil {
		return nil
	}
	defer C.xet_free_error(err)

	xetErr := &XetError{
		Code:    int(err.code),
		Message: C.GoString(err.message),
	}
	if err.details != nil {
		xetErr.Details = C.GoString(err.details)
	}

	return xetErr
}
