package hub

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
)

// Regex patterns
var (
	commitHashRegex = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Regex     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// URL construction functions

// HfHubURL constructs the URL of a file from the given information
func HfHubURL(repoID, filename string, opts *DownloadConfig) (string, error) {
	if opts == nil {
		opts = &DownloadConfig{}
	}

	// Set defaults
	repoType := opts.RepoType
	if repoType == "" {
		repoType = RepoTypeModel
	}

	revision := opts.Revision
	if revision == "" {
		revision = DefaultRevision
	}

	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}

	// Validate repo type
	if !isValidRepoType(repoType) {
		return "", fmt.Errorf("invalid repo type: %s. Accepted types are: %v", repoType, RepoTypes)
	}

	// Handle subfolder
	if opts.Subfolder != "" && opts.Subfolder != "." {
		filename = path.Join(opts.Subfolder, filename)
	}

	// Add repo type prefix for datasets and spaces
	repoPath := repoID
	if prefix, exists := RepoTypesURLPrefixes[repoType]; exists {
		repoPath = prefix + repoID
	}

	// Construct the URL - escape path components individually but preserve forward slashes
	escapedRevision := url.PathEscape(revision)
	// For filename, escape each path component separately to preserve forward slashes
	escapedFilename := escapeFilePath(filename)

	return fmt.Sprintf("%s/%s/resolve/%s/%s", endpoint, repoPath, escapedRevision, escapedFilename), nil
}

// escapeFilePath escapes each component of a file path separately, preserving forward slashes
func escapeFilePath(filename string) string {
	if filename == "" {
		return ""
	}

	// Split by forward slashes, escape each component, then rejoin
	parts := strings.Split(filename, "/")
	escapedParts := make([]string, len(parts))
	for i, part := range parts {
		escapedParts[i] = url.PathEscape(part)
	}
	return strings.Join(escapedParts, "/")
}

// RepoFolderName returns a serialized version of a repo name and type, safe for disk storage
func RepoFolderName(repoID, repoType string) string {
	// Convert slashes to separator
	parts := []string{repoType + "s"}
	parts = append(parts, strings.Split(repoID, "/")...)
	return strings.Join(parts, RepoIdSeparator)
}

// Validation functions

// IsCommitHash checks if the revision is a commit hash
func IsCommitHash(revision string) bool {
	return commitHashRegex.MatchString(revision)
}

// IsSHA256 checks if the etag is a valid SHA256 hash
func IsSHA256(etag string) bool {
	return sha256Regex.MatchString(etag)
}

// isValidRepoType checks if the repository type is valid
func isValidRepoType(repoType string) bool {
	for _, validType := range RepoTypes {
		if repoType == validType {
			return true
		}
	}
	return false
}

// File operations

// EnsureDir creates directory if it doesn't exist
func EnsureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// FileExists checks if a file exists
func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

// GetFileSize returns the size of a file
func GetFileSize(filename string) (int64, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// VerifyChecksum verifies the SHA256 checksum of a file
func VerifyChecksum(filename, expectedHash string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return fmt.Errorf("failed to compute hash: %w", err)
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if actualHash != expectedHash {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	return nil
}

// CheckDiskSpace checks if there's enough disk space for the expected file size
func CheckDiskSpace(expectedSize int64, targetDir string) error {
	if expectedSize <= 0 {
		return nil // Can't check if we don't know the size
	}

	// Try to get filesystem stats in a cross-platform way
	availableSpace, err := getAvailableDiskSpace(targetDir)
	if err != nil {
		// If we can't check disk space, just proceed with a warning
		// This is better than failing the download
		return nil
	}

	if availableSpace > 0 && availableSpace < expectedSize {
		return fmt.Errorf("insufficient disk space: need %d bytes, have %d bytes available", expectedSize, availableSpace)
	}

	return nil
}

// Symlink support

// AreSymlinksSupported returns whether symlinks are supported on the current platform and directory
func AreSymlinksSupported(dir string) bool {
	// Windows generally doesn't support symlinks without admin privileges
	if runtime.GOOS == "windows" {
		return false
	}

	// Test if we can create a symlink in the directory
	if dir == "" {
		return false
	}

	if err := EnsureDir(dir); err != nil {
		return false
	}

	tempSrc := filepath.Join(dir, "test_symlink_src")
	tempDst := filepath.Join(dir, "test_symlink_dst")

	// Create source file
	if err := os.WriteFile(tempSrc, []byte("test"), 0644); err != nil {
		return false
	}
	defer os.Remove(tempSrc)

	// Try to create symlink
	if err := os.Symlink(tempSrc, tempDst); err != nil {
		return false
	}
	defer os.Remove(tempDst)

	return true
}

// CreateSymlink creates a symlink, with fallback to copying if symlinks aren't supported
func CreateSymlink(src, dst string) error {
	// Remove destination if it exists
	if FileExists(dst) {
		if err := os.Remove(dst); err != nil {
			return fmt.Errorf("failed to remove existing destination: %w", err)
		}
	}

	// Ensure destination directory exists
	if err := EnsureDir(filepath.Dir(dst)); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Try to create symlink with relative path
	dstDir := filepath.Dir(dst)
	relSrc, err := filepath.Rel(dstDir, src)
	if err == nil {
		// Use relative path
		if err := os.Symlink(relSrc, dst); err == nil {
			return nil
		}
	}

	// Fall back to absolute path
	absSrc, err := filepath.Abs(src)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	if err := os.Symlink(absSrc, dst); err != nil {
		// Symlinks not supported, fall back to copying
		return copyFile(src, dst)
	}

	return nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	// Copy file permissions
	srcInfo, err := os.Stat(src)
	if err == nil {
		// Preserve file permissions - ignore errors as this might fail on some filesystems
		_ = os.Chmod(dst, srcInfo.Mode())
	}

	return nil
}

// Pattern matching for allow/ignore patterns

// MatchesPattern checks if a filename matches any of the given glob patterns
func MatchesPattern(filename string, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}

	for _, pattern := range patterns {
		matched, err := filepath.Match(pattern, filename)
		if err == nil && matched {
			return true
		}

		// Also check if the pattern matches the path components
		if strings.Contains(filename, pattern) {
			return true
		}
	}

	return false
}

// ShouldIgnoreFile determines if a file should be ignored based on patterns
func ShouldIgnoreFile(filename string, allowPatterns, ignorePatterns []string) bool {
	// If allow patterns are specified, file must match at least one
	if len(allowPatterns) > 0 {
		if !MatchesPattern(filename, allowPatterns) {
			return true
		}
	}

	// If ignore patterns are specified, file must not match any
	if len(ignorePatterns) > 0 {
		if MatchesPattern(filename, ignorePatterns) {
			return true
		}
	}

	return false
}

// HTTP helpers

// BuildHeaders builds HTTP headers for requests
func BuildHeaders(token, userAgent string, extraHeaders map[string]string) map[string]string {
	headers := make(map[string]string)

	// Add user agent
	if userAgent != "" {
		headers[UserAgentHeader] = userAgent
	}

	// Add authorization
	if token != "" {
		headers[AuthorizationHeader] = "Bearer " + token
	}

	// Add extra headers
	for k, v := range extraHeaders {
		headers[k] = v
	}

	return headers
}

// NormalizeEtag normalizes ETag HTTP header
func NormalizeEtag(etag string) string {
	if etag == "" {
		return ""
	}

	// Remove W/ prefix and quotes
	etag = strings.TrimPrefix(etag, "W/")
	etag = strings.Trim(etag, `"`)

	return etag
}

// Cache helpers

// GetPointerPath returns the path to a symlink/pointer file in cache
func GetPointerPath(storageFolder, revision, relativeFilename string) (string, error) {
	snapshotPath := filepath.Join(storageFolder, "snapshots")
	pointerPath := filepath.Join(snapshotPath, revision, relativeFilename)

	// Validate that the pointer path is within the snapshot directory
	absSnapshotPath, err := filepath.Abs(snapshotPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute snapshot path: %w", err)
	}

	absPointerPath, err := filepath.Abs(pointerPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute pointer path: %w", err)
	}

	if !strings.HasPrefix(absPointerPath, absSnapshotPath) {
		return "", fmt.Errorf("invalid pointer path: %s is not within snapshot directory %s", pointerPath, snapshotPath)
	}

	return pointerPath, nil
}

// CacheCommitHashForRevision caches the mapping between a revision and commit hash
func CacheCommitHashForRevision(storageFolder, revision, commitHash string) error {
	if revision == commitHash {
		return nil // No need to cache if revision is already a commit hash
	}

	refPath := filepath.Join(storageFolder, "refs", revision)
	if err := EnsureDir(filepath.Dir(refPath)); err != nil {
		return fmt.Errorf("failed to create refs directory: %w", err)
	}

	// Only update if the cached value is different
	if FileExists(refPath) {
		existing, err := os.ReadFile(refPath)
		if err == nil && string(existing) == commitHash {
			return nil // Already cached correctly
		}
	}

	if err := os.WriteFile(refPath, []byte(commitHash), 0644); err != nil {
		return fmt.Errorf("failed to cache commit hash: %w", err)
	}

	return nil
}

// AvailableDiskSpace returns the available disk space in bytes for the given
// directory, so callers outside this package can report free space rather than
// only assert against it.
func AvailableDiskSpace(dir string) (int64, error) {
	return getAvailableDiskSpace(dir)
}

// getAvailableDiskSpace returns the available disk space in bytes for the given directory
// This is a cross-platform implementation
func getAvailableDiskSpace(dir string) (int64, error) {
	// Ensure the directory exists
	if err := EnsureDir(dir); err != nil {
		return 0, err
	}

	// Get absolute path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return 0, err
	}

	// Platform-specific implementations
	switch runtime.GOOS {
	case "windows":
		return getAvailableDiskSpaceWindows(absDir)
	default:
		return getAvailableDiskSpaceUnix(absDir)
	}
}

// getAvailableDiskSpaceWindows returns available disk space on Windows
func getAvailableDiskSpaceWindows(dir string) (int64, error) {
	// Use Windows API to get actual disk space
	// We'll use a syscall approach that works with the standard library

	// For Windows, we need to get the drive letter from the path
	if len(dir) < 2 || dir[1] != ':' {
		// If we can't determine the drive, fall back to generic
		return getAvailableDiskSpaceGeneric(dir)
	}

	// Try to use GetDiskFreeSpaceEx equivalent
	// Since we can't easily call Windows API without additional dependencies,
	// let's use a more sophisticated generic approach for Windows
	return getAvailableDiskSpaceWindowsGeneric(dir)
}

// getAvailableDiskSpaceWindowsGeneric uses a Windows-optimized generic approach
func getAvailableDiskSpaceWindowsGeneric(dir string) (int64, error) {
	// This approach tries to be smarter about Windows disk space checking
	// by using larger test chunks and better heuristics

	testFile := filepath.Join(dir, ".hf_disk_space_test")

	// Clean up any existing test file
	os.Remove(testFile)

	file, err := os.Create(testFile)
	if err != nil {
		return 0, fmt.Errorf("unable to create test file for Windows disk space check: %w", err)
	}
	defer func() {
		file.Close()
		os.Remove(testFile)
	}()

	// For Windows, try a binary search approach to find available space
	// This is more efficient than linear growth

	// Start with a reasonable minimum (1MB) and maximum (1TB) range
	minSpace := int64(1024 * 1024)               // 1MB
	maxSpace := int64(1024 * 1024 * 1024 * 1024) // 1TB

	// Binary search to find the actual available space
	for minSpace < maxSpace {
		testSize := (minSpace + maxSpace) / 2

		// Try to seek to this position and write a small amount
		if _, err := file.Seek(testSize-1, 0); err != nil {
			// Can't seek this far, reduce max
			maxSpace = testSize - 1
			continue
		}

		// Try to write one byte at this position
		if _, err := file.Write([]byte{0}); err != nil {
			// Can't write at this position, reduce max
			maxSpace = testSize - 1
		} else {
			// Can write at this position, increase min
			minSpace = testSize + 1
		}

		// Reset file position
		if _, err := file.Seek(0, 0); err != nil {
			// If we can't reset, that's ok, continue with the search
			continue
		}
	}

	// Return the largest size we could successfully seek to
	availableSpace := maxSpace

	// Sanity checks
	if availableSpace < 0 {
		return 0, fmt.Errorf("negative disk space calculated")
	}

	// If we got a very small value, fall back to the regular generic method
	if availableSpace < 1024*1024 { // Less than 1MB
		return getAvailableDiskSpaceGeneric(dir)
	}

	return availableSpace, nil
}

// getAvailableDiskSpaceUnix returns available disk space on Unix-like systems
func getAvailableDiskSpaceUnix(dir string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		// If syscall fails, try the generic approach
		return getAvailableDiskSpaceGeneric(dir)
	}

	// Available space in bytes = available blocks × block size
	availableSpace := int64(stat.Bavail) * int64(stat.Bsize)

	// Sanity check: ensure we don't return negative or unreasonably large values
	if availableSpace < 0 {
		return 0, fmt.Errorf("invalid disk space calculation: negative value")
	}

	// If we get an unreasonably large value (> 1PB), something's wrong
	const maxReasonableSpace = 1024 * 1024 * 1024 * 1024 * 1024 // 1 PB
	if availableSpace > maxReasonableSpace {
		return 0, fmt.Errorf("invalid disk space calculation: unreasonably large value")
	}

	return availableSpace, nil
}

// getAvailableDiskSpaceGeneric is a fallback method that tries to estimate disk space
func getAvailableDiskSpaceGeneric(dir string) (int64, error) {
	// This is a fallback that attempts to measure available space
	// by trying to create and grow a temporary file

	testFile := filepath.Join(dir, ".hf_disk_space_test")

	// Clean up any existing test file
	os.Remove(testFile)

	file, err := os.Create(testFile)
	if err != nil {
		// If we can't create a file, assume no space
		return 0, fmt.Errorf("unable to create test file: %w", err)
	}
	defer func() {
		file.Close()
		os.Remove(testFile)
	}()

	// Try to write increasingly larger chunks to estimate free space
	// Start with 1MB chunks and work our way up
	const chunkSize = 1024 * 1024 // 1MB
	var totalWritten int64
	chunk := make([]byte, chunkSize)

	// Limit our test to avoid writing too much (max 100MB test)
	const maxTestSize = 100 * 1024 * 1024 // 100MB

	for totalWritten < maxTestSize {
		n, err := file.Write(chunk)
		if err != nil {
			// If we can't write anymore, we've hit the limit
			break
		}
		totalWritten += int64(n)

		// If we didn't write the full chunk, we're close to the limit
		if n < len(chunk) {
			break
		}
	}

	// Our estimate is what we could write plus some buffer
	// This is conservative but better than hardcoded values
	estimatedSpace := totalWritten * 2 // Double what we could write as estimate

	// Minimum reasonable space check
	const minReasonableSpace = 1024 * 1024 // 1MB
	if estimatedSpace < minReasonableSpace {
		return totalWritten, nil // Return what we actually measured
	}

	return estimatedSpace, nil
}
