package modelagent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArtifactPathResolutionPreservesFilesystemSpelling(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	linked := filepath.Join(root, "physical", "child")
	require.NoError(t, os.MkdirAll(linked, 0700))
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(linked, alias))
	for _, spelling := range []string{"parent after alias", "repeated slash", "space", "relative"} {
		t.Run(spelling, func(t *testing.T) {
			path := alias + "/../missing"
			want := filepath.Join(root, "physical", "missing")
			switch spelling {
			case "repeated slash":
				path = root + "/hf://org/model"
				want = filepath.Join(root, "hf:", "org", "model")
			case "space":
				path, want = root+"/literal ", root+"/literal "
			case "relative":
				cwd, err := os.Getwd()
				require.NoError(t, err)
				relative, err := filepath.Rel(cwd, alias)
				require.NoError(t, err)
				path = relative + "/../missing"
			}
			for _, writer := range []bool{false, true} {
				got, err := resolveArtifactPathWithMissingLinks(path, writer)
				require.NoError(t, err)
				require.Equal(t, want, got)
				got, err = walkArtifactPath(path, writer, func(string, bool) error { return nil })
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
	_, err = resolveArtifactPath(root + "/absent/../elsewhere")
	require.Error(t, err, "an unresolved parent traversal cannot be silently cleaned")
}
