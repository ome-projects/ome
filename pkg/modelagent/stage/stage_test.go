package stage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeModel(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"a":1}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "nested", "model.safetensors"), []byte("weights"), 0o644))
}

func newSource(t *testing.T) (root, src string) {
	t.Helper()
	root = t.TempDir()
	src = filepath.Join(root, "qwen", "Qwen3-32B")
	writeModel(t, src)
	return root, src
}

func TestRunCopiesTreeToDestination(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")

	result, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	assert.True(t, result.Copied)
	assert.Equal(t, []byte(`{"a":1}`), mustRead(t, filepath.Join(dest, "config.json")))
	assert.Equal(t, []byte("weights"), mustRead(t, filepath.Join(dest, "nested", "model.safetensors")))
}

func TestRunReportsBytesCopied(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")

	result, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	assert.Equal(t, int64(len(`{"a":1}`)+len("weights")), result.BytesCopied)
}

func TestRunWritesCompletionMarker(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(dest, MarkerFileName))
}

func TestRunLeavesNoStagingDirectoryBehind(t *testing.T) {
	root, src := newSource(t)
	destParent := t.TempDir()
	dest := filepath.Join(destParent, "qwen3-32b")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	entries, err := os.ReadDir(destParent)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "qwen3-32b", entries[0].Name())
}

func TestRunReusesCompletedStage(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")
	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})
	require.NoError(t, err)
	// A second copy would overwrite this marker file we planted in the destination.
	require.NoError(t, os.WriteFile(filepath.Join(dest, "config.json"), []byte("untouched"), 0o644))

	result, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	assert.False(t, result.Copied)
	assert.Equal(t, []byte("untouched"), mustRead(t, filepath.Join(dest, "config.json")))
}

func TestRunRecopiesWhenAlwaysCopyRequested(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")
	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dest, "config.json"), []byte("stale"), 0o644))

	result, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}, AlwaysCopy: true})

	require.NoError(t, err)
	assert.True(t, result.Copied)
	assert.Equal(t, []byte(`{"a":1}`), mustRead(t, filepath.Join(dest, "config.json")))
}

// An interrupted copy leaves a directory with no marker. Because the agent only
// stats the path, treating it as complete would serve truncated weights.
func TestRunRestagesDestinationWithoutMarker(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")
	require.NoError(t, os.MkdirAll(dest, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dest, "config.json"), []byte("half"), 0o644))

	result, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	assert.True(t, result.Copied)
	assert.Equal(t, []byte(`{"a":1}`), mustRead(t, filepath.Join(dest, "config.json")))
}

func TestRunRestagesWhenMarkerNamesDifferentSource(t *testing.T) {
	root, src := newSource(t)
	other := filepath.Join(root, "qwen", "Qwen3-8B")
	writeModel(t, other)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")
	_, err := Run(context.Background(), other, dest, Options{SourceRoots: []string{root}})
	require.NoError(t, err)

	result, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	assert.True(t, result.Copied)
}

func TestRunRejectsSourceOutsideRoots(t *testing.T) {
	_, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{t.TempDir()}})

	assert.ErrorContains(t, err, "outside the configured stage source roots")
}

func TestRunRejectsDestinationInsideSource(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(src, "copy")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	assert.ErrorContains(t, err, "must not be inside")
}

func TestRunRejectsDestinationEqualToSource(t *testing.T) {
	root, src := newSource(t)

	_, err := Run(context.Background(), src, src, Options{SourceRoots: []string{root}})

	assert.ErrorContains(t, err, "must not be inside")
}

// A failed copy must not leave a destination behind: the node label reconciler
// would then advertise the model as present.
func TestRunLeavesNoDestinationWhenCopyFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the mode bits this test relies on to fail the copy")
	}
	root, src := newSource(t)
	unreadable := filepath.Join(src, "locked.bin")
	require.NoError(t, os.WriteFile(unreadable, []byte("secret"), 0o000))
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o644) })
	destParent := t.TempDir()
	dest := filepath.Join(destParent, "qwen3-32b")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.Error(t, err)
	assert.NoDirExists(t, dest)
	entries, readErr := os.ReadDir(destParent)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "staging directory should be cleaned up")
}

func TestDirSizeSumsRegularFiles(t *testing.T) {
	_, src := newSource(t)

	size, err := DirSize(src)

	require.NoError(t, err)
	assert.Equal(t, int64(len(`{"a":1}`)+len("weights")), size)
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

// MkdirTemp creates 0700, which after the rename would leave a model directory
// only root can traverse — inference runtimes that drop privileges could not
// read the weights.
func TestRunPublishesTraversableDestination(t *testing.T) {
	root, src := newSource(t)
	dest := filepath.Join(t.TempDir(), "qwen3-32b")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	info, err := os.Stat(dest)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

// A symlink is staged verbatim, so one pointing outside the configured roots
// would hand inference pods a path the allowlist exists to deny.
func TestRunRejectsSymlinkEscapingRootsInsideSource(t *testing.T) {
	root, src := newSource(t)
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	require.NoError(t, os.WriteFile(secret, []byte("secret"), 0o644))
	require.NoError(t, os.Symlink(secret, filepath.Join(src, "leak")))
	dest := filepath.Join(t.TempDir(), "qwen3-32b")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	assert.ErrorContains(t, err, "outside the configured stage source roots")
	assert.NoDirExists(t, dest)
}

func TestRunKeepsSymlinkResolvingInsideRoots(t *testing.T) {
	root, src := newSource(t)
	require.NoError(t, os.Symlink("config.json", filepath.Join(src, "config-link.json")))
	dest := filepath.Join(t.TempDir(), "qwen3-32b")

	_, err := Run(context.Background(), src, dest, Options{SourceRoots: []string{root}})

	require.NoError(t, err)
	target, err := os.Readlink(filepath.Join(dest, "config-link.json"))
	require.NoError(t, err)
	assert.Equal(t, "config.json", target)
}

// Staging a large model takes minutes; a cancelled task must stop copying
// rather than run to completion against a share nobody is waiting on.
func TestRunStopsOnCancelledContext(t *testing.T) {
	root, src := newSource(t)
	destParent := t.TempDir()
	dest := filepath.Join(destParent, "qwen3-32b")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Run(ctx, src, dest, Options{SourceRoots: []string{root}})

	require.ErrorIs(t, err, context.Canceled)
	assert.NoDirExists(t, dest)
	entries, readErr := os.ReadDir(destParent)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "staging directory should be cleaned up")
}
