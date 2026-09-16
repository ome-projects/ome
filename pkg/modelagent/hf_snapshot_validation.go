package modelagent

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"sigs.k8s.io/ome/pkg/constants"
)

const hfSnapshotMaxMetadataBytes = 16 << 20

// blobId identifies the Git blob, which is an LFS pointer for large files.
// Only lfs.sha256 identifies the downloaded contents of an LFS file.
type hfSnapshotManifest struct {
	SHA   string           `json:"sha"`
	Files []hfSnapshotFile `json:"siblings"`
}

type hfSnapshotFile struct {
	Name   string         `json:"rfilename"`
	Size   *int64         `json:"size"`
	BlobID string         `json:"blobId"`
	LFS    *hfSnapshotLFS `json:"lfs,omitempty"`
}

type hfSnapshotLFS struct {
	SHA256 string `json:"sha256"`
	Size   *int64 `json:"size"`
}

func fetchHfSnapshotManifest(ctx context.Context, modelID, revision, token, endpoint string) (hfSnapshotManifest, error) {
	var manifest hfSnapshotManifest
	identity, err := newHfArtifactIdentity(modelID, revision)
	if err != nil {
		return manifest, err
	}
	metadataURL, err := hfRevisionMetadataURL(identity.ModelID, identity.CommitSHA, endpoint)
	if err != nil {
		return manifest, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL+"?blobs=true", nil)
	if err != nil {
		return manifest, err
	}
	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := NewHTTPClientWithTimeout(DefaultRequestTimeout)
	client.CheckRedirect = checkHfMetadataRedirect
	response, err := client.Do(req)
	if err != nil {
		return manifest, fmt.Errorf("fetch HF snapshot manifest: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return manifest, fmt.Errorf("fetch HF snapshot manifest: HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, hfSnapshotMaxMetadataBytes+1))
	if err != nil {
		return manifest, err
	}
	if len(body) > hfSnapshotMaxMetadataBytes {
		return manifest, fmt.Errorf("HF snapshot manifest exceeds size limit")
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return manifest, fmt.Errorf("decode HF snapshot manifest: %w", err)
	}
	return manifest, manifest.check(identity.CommitSHA)
}

func (manifest hfSnapshotManifest) check(revision string) error {
	if !isHfSnapshotDigest(manifest.SHA, sha1.Size) || !strings.EqualFold(manifest.SHA, revision) || len(manifest.Files) == 0 {
		return fmt.Errorf("HF snapshot manifest is empty or does not match revision %s", revision)
	}
	seen := make(map[string]bool, len(manifest.Files))
	for _, file := range manifest.Files {
		if file.Name == "" || path.IsAbs(file.Name) || strings.ContainsAny(file.Name, "\\\x00") ||
			constants.IsInternalArtifactObjectName(file.Name) || path.Base(file.Name) == constants.HfArtifactReadyMarkerFileName {
			return fmt.Errorf("unsafe HF snapshot filename %q", file.Name)
		}
		for _, part := range strings.Split(file.Name, "/") {
			if part == "" || part == "." || part == ".." || part == constants.ModelArtifactsDirectory || part == constants.HfArtifactReadyMarkerFileName {
				return fmt.Errorf("unsafe HF snapshot filename %q", file.Name)
			}
		}
		if seen[file.Name] {
			return fmt.Errorf("duplicate HF snapshot filename %q", file.Name)
		}
		seen[file.Name] = true
		if file.Size == nil || *file.Size < 0 {
			return fmt.Errorf("HF snapshot file %q has no valid size", file.Name)
		}
		if file.LFS != nil {
			if file.LFS.Size == nil || *file.LFS.Size != *file.Size || !isHfSnapshotDigest(file.LFS.SHA256, sha256.Size) {
				return fmt.Errorf("HF snapshot file %q has invalid LFS metadata", file.Name)
			}
		} else if !isHfSnapshotDigest(file.BlobID, sha1.Size) {
			return fmt.Errorf("HF snapshot file %q has no valid Git blob ID", file.Name)
		}
	}
	for name := range seen {
		for dir := path.Dir(name); dir != "."; dir = path.Dir(dir) {
			if seen[dir] {
				return fmt.Errorf("HF snapshot path %q is both file and directory", dir)
			}
		}
	}
	return nil
}

func isHfSnapshotDigest(value string, size int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == size
}

func (manifest hfSnapshotManifest) validate(ctx context.Context, directory string) (bool, error) {
	invalid, err := manifest.invalidFiles(ctx, directory)
	return len(invalid) == 0 && err == nil, err
}

// removeInvalidFiles is a write operation: shared callers must hold the parent
// operation and have failed its children before calling it. Validation is read-only.
func (manifest hfSnapshotManifest) removeInvalidFiles(ctx context.Context, directory string) error {
	invalid, err := manifest.invalidFiles(ctx, directory)
	if err != nil || len(invalid) == 0 {
		return err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, name := range invalid {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Root.Remove unlinks leaf symlinks without following their targets.
		if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove invalid HF snapshot file %q: %w", name, err)
		}
	}
	return nil
}

func (manifest hfSnapshotManifest) invalidFiles(ctx context.Context, directory string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := manifest.check(manifest.SHA); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if os.IsNotExist(err) {
		missing := make([]string, 0, len(manifest.Files))
		for _, file := range manifest.Files {
			missing = append(missing, file.Name)
		}
		return missing, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("HF snapshot root %s is not a real directory", directory)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	var invalid []string
	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Xet writes by path. Reject symlinked directories even if their target
		// currently stays within the snapshot; never allow it to write through one.
		for dir := path.Dir(file.Name); dir != "."; dir = path.Dir(dir) {
			info, err := root.Lstat(dir)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("HF snapshot directory %q is not a real directory", dir)
			}
		}
		info, err := root.Lstat(file.Name)
		if os.IsNotExist(err) {
			invalid = append(invalid, file.Name)
			continue
		}
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			invalid = append(invalid, file.Name)
			continue
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("HF snapshot file %q is not a regular file", file.Name)
		}
		if info.Size() != *file.Size {
			invalid = append(invalid, file.Name)
			continue
		}
		var digest hash.Hash = sha1.New()
		expected := file.BlobID
		if file.LFS != nil {
			digest, expected = sha256.New(), file.LFS.SHA256
		} else {
			fmt.Fprintf(digest, "blob %d\x00", *file.Size)
		}
		reader, err := root.Open(file.Name)
		if err != nil {
			return nil, err
		}
		size, readErr := io.Copy(digest, hfSnapshotContextReader{ctx, io.LimitReader(reader, *file.Size+1)})
		closeErr := reader.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if size != *file.Size || !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), expected) {
			invalid = append(invalid, file.Name)
		}
	}
	return invalid, nil
}

type hfSnapshotContextReader struct {
	ctx context.Context
	io.Reader
}

func (reader hfSnapshotContextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.Reader.Read(buffer)
}
