package stage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveSourceRejectsWhenNoRootsConfigured(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "qwen")
	require.NoError(t, os.MkdirAll(src, 0o755))

	_, err := ResolveSource(src, nil)

	assert.ErrorContains(t, err, "no stage source roots configured")
}

func TestResolveSourceAcceptsPathUnderRoot(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "qwen", "Qwen3-32B")
	require.NoError(t, os.MkdirAll(src, 0o755))

	got, err := ResolveSource(src, []string{root})

	require.NoError(t, err)
	assert.Equal(t, mustEvalSymlinks(t, src), got)
}

func TestResolveSourceRejectsPathOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	src := filepath.Join(outside, "qwen")
	require.NoError(t, os.MkdirAll(src, 0o755))

	_, err := ResolveSource(src, []string{root})

	assert.ErrorContains(t, err, "outside the configured stage source roots")
}

// A root of /mnt/nfs must not authorize /mnt/nfs-evil: plain string prefixing
// would let a sibling directory through.
func TestResolveSourceRejectsSiblingWithRootAsStringPrefix(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "nfs")
	sibling := filepath.Join(parent, "nfs-evil")
	require.NoError(t, os.MkdirAll(root, 0o755))
	require.NoError(t, os.MkdirAll(sibling, 0o755))

	_, err := ResolveSource(sibling, []string{root})

	assert.ErrorContains(t, err, "outside the configured stage source roots")
}

// A symlink living under the root but pointing outside it must be rejected:
// the containment check has to run on the resolved path, not the declared one.
func TestResolveSourceRejectsSymlinkEscapingRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secrets := filepath.Join(outside, "secrets")
	require.NoError(t, os.MkdirAll(secrets, 0o755))

	link := filepath.Join(root, "escape")
	require.NoError(t, os.Symlink(secrets, link))

	_, err := ResolveSource(link, []string{root})

	assert.ErrorContains(t, err, "outside the configured stage source roots")
}

func TestResolveSourceAcceptsRootItself(t *testing.T) {
	root := t.TempDir()

	got, err := ResolveSource(root, []string{root})

	require.NoError(t, err)
	assert.Equal(t, mustEvalSymlinks(t, root), got)
}

func TestResolveSourceRejectsMissingPath(t *testing.T) {
	root := t.TempDir()

	_, err := ResolveSource(filepath.Join(root, "absent"), []string{root})

	assert.ErrorContains(t, err, "does not exist")
}

func TestResolveSourceRejectsFile(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "config.json")
	require.NoError(t, os.WriteFile(file, []byte("{}"), 0o644))

	_, err := ResolveSource(file, []string{root})

	assert.ErrorContains(t, err, "not a directory")
}

func mustEvalSymlinks(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)
	return resolved
}
