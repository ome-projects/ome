package xet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// HF Hub compatibility types to match pkg/hfutil/hub

// DownloadConfig matches the structure from pkg/hfutil/hub
type DownloadConfig struct {
	RepoID         string
	RepoType       string
	Revision       string
	CacheDir       string
	LocalDir       string
	Filename       string
	Endpoint       string
	Token          string
	MaxWorkers     int
	AllowPatterns  []string
	IgnorePatterns []string
	EtagTimeout    int
	ForceDownload  bool
	ForceFilename  string
	ProxiesAuth    map[string]string
	LocalFilesOnly bool
	LogLevel       string // Optional: error, warn, info, debug, trace
}

// Global client for compatibility
var globalClient *Client

// Initialize global client with default config
func init() {
	if os.Getenv("XET_DISABLE_GLOBAL_CLIENT") == "1" {
		return
	}
	// Try to get token from environment
	token := os.Getenv("HF_TOKEN")
	if token == "" {
		token = os.Getenv("HUGGING_FACE_HUB_TOKEN")
	}

	// Try to get endpoint from environment
	endpoint := os.Getenv("HF_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://huggingface.co"
	}

	// Try to get cache dir from environment
	cacheDir := os.Getenv("HF_HOME")
	if cacheDir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			cacheDir = filepath.Join(home, ".cache", "huggingface")
		}
	}

	logLevel := os.Getenv("XET_LOG_LEVEL")

	config := &Config{
		Endpoint:               endpoint,
		Token:                  token,
		CacheDir:               cacheDir,
		MaxConcurrentDownloads: 4,
		EnableDedup:            true,
		LogLevel:               logLevel,
	}

	globalClient, _ = NewClient(config)
}

// HfHubDownload provides compatibility with the existing HF Hub download function
func HfHubDownload(ctx context.Context, config *DownloadConfig) (string, error) {
	// Validate input
	if config.RepoID == "" {
		return "", fmt.Errorf("repo_id cannot be empty")
	}
	if config.Filename == "" {
		return "", fmt.Errorf("filename cannot be empty")
	}

	// Create client if needed
	client := globalClient
	if config.Token != "" || config.Endpoint != "" || config.CacheDir != "" {
		// Create a custom client with the provided config
		xetConfig := &Config{
			Endpoint:               config.Endpoint,
			Token:                  config.Token,
			CacheDir:               config.CacheDir,
			MaxConcurrentDownloads: uint32(config.MaxWorkers),
			EnableDedup:            true,
		}

		if xetConfig.Endpoint == "" {
			xetConfig.Endpoint = "https://huggingface.co"
		}
		if xetConfig.MaxConcurrentDownloads == 0 {
			xetConfig.MaxConcurrentDownloads = 4
		}

		var err error
		client, err = NewClient(xetConfig)
		if err != nil {
			return "", err
		}
		defer client.Close()
	}

	if client == nil {
		return "", fmt.Errorf("failed to create xet client")
	}

	// Convert to xet download request
	req := &DownloadRequest{
		RepoID:   config.RepoID,
		RepoType: config.RepoType,
		Revision: config.Revision,
		Filename: config.Filename,
		LocalDir: config.LocalDir,
	}

	// Set defaults
	if req.RepoType == "" {
		req.RepoType = "models"
	}
	if req.Revision == "" {
		req.Revision = "main"
	}

	// Use context-aware download if context is provided
	if ctx != nil {
		return client.DownloadFileWithContext(ctx, req)
	}

	return client.DownloadFile(req)
}

// SnapshotDownload provides compatibility with the existing snapshot download function
func SnapshotDownload(ctx context.Context, config *DownloadConfig) (string, error) {
	if config.LocalDir == "" {
		return "", fmt.Errorf("local_dir must be specified for snapshot download")
	}

	// Create client
	client := globalClient
	if config.Token != "" || config.Endpoint != "" || config.CacheDir != "" {
		xetConfig := &Config{
			Endpoint:               config.Endpoint,
			Token:                  config.Token,
			CacheDir:               config.CacheDir,
			MaxConcurrentDownloads: uint32(config.MaxWorkers),
			EnableDedup:            true,
		}

		if xetConfig.Endpoint == "" {
			xetConfig.Endpoint = "https://huggingface.co"
		}
		if xetConfig.MaxConcurrentDownloads == 0 {
			xetConfig.MaxConcurrentDownloads = 4
		}

		var err error
		client, err = NewClient(xetConfig)
		if err != nil {
			return "", err
		}
		defer client.Close()
	}

	if client == nil {
		return "", fmt.Errorf("failed to create xet client")
	}

	revision := config.Revision
	if revision == "" {
		revision = "main"
	}

	snapshotReq := &SnapshotRequest{
		RepoID:   config.RepoID,
		RepoType: config.RepoType,
		Revision: revision,
		LocalDir: config.LocalDir,
	}

	if snapshotReq.RepoType == "" {
		snapshotReq.RepoType = "models"
	}

	return client.DownloadSnapshotWithContext(ctx, snapshotReq)
}

// SnapshotDownloadWithProgress downloads a model snapshot with progress callbacks.
// The progressHandler is called periodically with download progress updates.
// The throttle parameter controls how often progress updates are sent (minimum 200ms).
func SnapshotDownloadWithProgress(
	ctx context.Context,
	config *DownloadConfig,
	progressHandler ProgressHandler,
	throttle time.Duration,
) (string, error) {
	if config.LocalDir == "" {
		return "", fmt.Errorf("local_dir must be specified for snapshot download")
	}

	// Create client with custom config
	xetConfig := &Config{
		Endpoint:               config.Endpoint,
		Token:                  config.Token,
		CacheDir:               config.CacheDir,
		MaxConcurrentDownloads: uint32(config.MaxWorkers),
		EnableDedup:            true,
	}

	if xetConfig.Endpoint == "" {
		xetConfig.Endpoint = "https://huggingface.co"
	}
	if xetConfig.MaxConcurrentDownloads == 0 {
		xetConfig.MaxConcurrentDownloads = 4
	}

	client, err := NewClient(xetConfig)
	if err != nil {
		return "", err
	}
	defer client.Close()

	// Set up progress handler if provided
	if progressHandler != nil {
		if throttle <= 0 {
			throttle = 200 * time.Millisecond
		}
		if err := client.SetProgressHandler(progressHandler, throttle); err != nil {
			return "", fmt.Errorf("failed to set progress handler: %w", err)
		}
	}

	revision := config.Revision
	if revision == "" {
		revision = "main"
	}

	snapshotReq := &SnapshotRequest{
		RepoID:   config.RepoID,
		RepoType: config.RepoType,
		Revision: revision,
		LocalDir: config.LocalDir,
	}

	if snapshotReq.RepoType == "" {
		snapshotReq.RepoType = "models"
	}

	return client.DownloadSnapshotWithContext(ctx, snapshotReq)
}

// ListRepoFiles lists files in a repository (compatibility function)
func ListRepoFiles(ctx context.Context, config *DownloadConfig) ([]FileInfo, error) {
	client := globalClient
	if client == nil {
		return nil, fmt.Errorf("xet client not initialized")
	}

	revision := config.Revision
	if revision == "" {
		revision = "main"
	}

	return client.ListFiles(config.RepoID, revision)
}

// ToDownloadConfig converts Config to DownloadConfig
func (c *Config) ToDownloadConfig() *DownloadConfig {
	return &DownloadConfig{
		Token:      c.Token,
		CacheDir:   c.CacheDir,
		Endpoint:   c.Endpoint,
		MaxWorkers: int(c.MaxConcurrentDownloads),
		// Set sensible defaults for common fields
		Revision: "main",        // Default git branch
		RepoType: RepoTypeModel, // Most common repository type
	}
}
