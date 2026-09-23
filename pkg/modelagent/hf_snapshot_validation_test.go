package modelagent

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/constants"
)

const testDirectHfSHA = "0123456789abcdef0123456789abcdef01234567"

func testHfSnapshotManifest(contents map[string]string) hfSnapshotManifest {
	manifest := hfSnapshotManifest{SHA: testDirectHfSHA}
	for name, data := range contents {
		size := int64(len(data))
		file := hfSnapshotFile{Name: name, Size: &size,
			BlobID: fmt.Sprintf("%x", sha1.Sum([]byte(fmt.Sprintf("blob %d\x00%s", size, data))))}
		if strings.HasSuffix(name, ".safetensors") {
			// The Git OID above deliberately differs from the actual LFS digest.
			file.LFS = &hfSnapshotLFS{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(data))), Size: &size}
		}
		manifest.Files = append(manifest.Files, file)
	}
	return manifest
}

func TestHfSnapshotValidationDigestsAndReadOnlyCorruption(t *testing.T) {
	root := t.TempDir()
	contents := map[string]string{"config.json": "{}", "model.safetensors": "weights", "empty": ""}
	manifest := testHfSnapshotManifest(contents)
	for name, data := range contents {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(data), 0o600))
	}
	valid, err := manifest.validate(context.Background(), root)
	require.NoError(t, err)
	require.True(t, valid)
	for name, bad := range map[string]string{"config.json": "[]", "model.safetensors": "corrupt"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(bad), 0o600))
	}
	valid, err = manifest.validate(context.Background(), root)
	require.NoError(t, err)
	assert.False(t, valid)
	data, err := os.ReadFile(filepath.Join(root, "model.safetensors"))
	require.NoError(t, err)
	assert.Equal(t, "corrupt", string(data), "validation must not modify bytes")
	require.NoError(t, manifest.removeInvalidFiles(context.Background(), root))
	assert.NoFileExists(t, filepath.Join(root, "model.safetensors"))
	assert.NoFileExists(t, filepath.Join(root, "config.json"))
	assert.FileExists(t, filepath.Join(root, "empty"))
}

func TestHfSnapshotValidationRejectsUnsafeManifest(t *testing.T) {
	for _, name := range []string{"", "/absolute", "../escape", "a/../escape", "a//b", "a/./b", "a\\b", "a\x00b", constants.ModelArtifactsDirectory + "/weights"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			manifest := testHfSnapshotManifest(map[string]string{name: "x"})
			assert.Error(t, manifest.check(testDirectHfSHA))
		})
	}
	for _, mutate := range []func(*hfSnapshotManifest){
		func(m *hfSnapshotManifest) { m.SHA = strings.Repeat("a", 40) },
		func(m *hfSnapshotManifest) { m.Files = nil },
		func(m *hfSnapshotManifest) { m.Files = append(m.Files, m.Files[0]) },
		func(m *hfSnapshotManifest) { m.Files[0].Size = nil },
		func(m *hfSnapshotManifest) { m.Files[0].BlobID = "missing" },
		func(m *hfSnapshotManifest) { m.Files[0].LFS = &hfSnapshotLFS{SHA256: strings.Repeat("a", 64)} },
	} {
		manifest := testHfSnapshotManifest(map[string]string{"config.json": "{}"})
		mutate(&manifest)
		assert.Error(t, manifest.check(testDirectHfSHA))
	}
}

func TestHfSnapshotManifestRejectsLocalReadyMarker(t *testing.T) {
	for _, name := range []string{constants.HfArtifactReadyMarkerFileName, "nested/" + constants.HfArtifactReadyMarkerFileName, constants.HfArtifactReadyMarkerFileName + "/child"} {
		manifest := testHfSnapshotManifest(map[string]string{name: "repository-controlled content"})
		assert.ErrorContains(t, manifest.check(testDirectHfSHA), "unsafe HF snapshot filename")
	}
}

func TestFetchHfSnapshotManifestHonorsCallerDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := fetchHfSnapshotManifest(ctx, "Org/Model", testDirectHfSHA, "", server.URL)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestHfSnapshotValidationNeverFollowsSymlinks(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "weights"), []byte("outside"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "nested")))
	manifest := testHfSnapshotManifest(map[string]string{"nested/weights": "weights"})
	_, err := manifest.validate(context.Background(), root)
	require.Error(t, err)
	assert.Error(t, manifest.removeInvalidFiles(context.Background(), root))
	data, err := os.ReadFile(filepath.Join(outside, "weights"))
	require.NoError(t, err)
	assert.Equal(t, "outside", string(data))
	manifest = testHfSnapshotManifest(map[string]string{"leaf": "weights"})
	require.NoError(t, os.Symlink(filepath.Join(outside, "weights"), filepath.Join(root, "leaf")))
	valid, err := manifest.validate(context.Background(), root)
	require.NoError(t, err)
	assert.False(t, valid)
	require.NoError(t, manifest.removeInvalidFiles(context.Background(), root))
	assert.FileExists(t, filepath.Join(outside, "weights"))
	_, err = manifest.validate(context.Background(), filepath.Join(root, "nested"))
	assert.Error(t, err, "snapshot root cannot itself be a symlink")
}

func TestFetchHfSnapshotManifest(t *testing.T) {
	manifest := testHfSnapshotManifest(map[string]string{"config.json": "{}", "model.safetensors": "weights"})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/hub/api/models/Org/Model/revision/"+testDirectHfSHA, r.URL.Path)
		assert.Equal(t, "true", r.URL.Query().Get("blobs"))
		assert.Equal(t, "Bearer effective-token", r.Header.Get("Authorization"))
		require.NoError(t, json.NewEncoder(w).Encode(manifest))
	}))
	defer server.Close()
	got, err := fetchHfSnapshotManifest(context.Background(), "Org/Model", testDirectHfSHA, "effective-token", server.URL+"/hub")
	require.NoError(t, err)
	assert.Equal(t, manifest, got)
	_, err = fetchHfSnapshotManifest(context.Background(), "Org/Model", "refs/pr/1", "", server.URL)
	assert.Error(t, err, "manifests require an immutable revision")
}

func TestFetchHfSnapshotManifestRejectsUntrustedResponses(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusNotFound, http.StatusFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "https://example.invalid/token-leak")
				w.WriteHeader(status)
			}))
			defer server.Close()
			_, err := fetchHfSnapshotManifest(context.Background(), "Org/Model", testDirectHfSHA, "secret", server.URL)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret")
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fetchHfSnapshotManifest(ctx, "Org/Model", testDirectHfSHA, "", "https://example.invalid")
	assert.ErrorIs(t, err, context.Canceled)
	manifest := testHfSnapshotManifest(map[string]string{"config.json": "{}"})
	_, err = manifest.validate(ctx, t.TempDir())
	assert.ErrorIs(t, err, context.Canceled)
}
