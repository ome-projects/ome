package modelagent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectOperationPathBoundaries(t *testing.T) {
	for _, boundary := range []string{"owned", "external legacy", "reserved", "root", "missing UID", "external ancestor alias", "external leaf alias", "managed alias"} {
		t.Run(boundary, func(t *testing.T) {
			g, task := newArtifactRequestValidationTest(t)
			root, err := filepath.EvalSymlinks(t.TempDir())
			require.NoError(t, err)
			g.modelRootDir = root
			path := filepath.Join(root, "family", "model")
			require.NoError(t, os.MkdirAll(path, 0o700))
			var expected = path
			var rejected bool
			switch boundary {
			case "external legacy":
				path, expected = filepath.Join(t.TempDir(), "external"), ""
			case "reserved":
				path, rejected = filepath.Join(root, "_artifacts", "model"), true
			case "root":
				path, rejected = root, true
			case "missing UID":
				task.BaseModel.UID, rejected = "", true
			case "external ancestor alias":
				alias := filepath.Join(t.TempDir(), "alias")
				require.NoError(t, os.Symlink(filepath.Dir(path), alias))
				path = filepath.Join(alias, "model")
			case "external leaf alias":
				alias := filepath.Join(t.TempDir(), "alias")
				require.NoError(t, os.Symlink(path, alias))
				path, rejected = alias, true
			case "managed alias":
				alias := filepath.Join(root, "family", "alias")
				require.NoError(t, os.Symlink(filepath.Dir(path), alias))
				path, rejected = filepath.Join(alias, "model"), true
			}
			task.BaseModel.Spec.Storage.Path = &path
			actual, err := g.directArtifactOperationPath(task, false)
			if rejected {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func TestDirectPathLockSharesExistingFamilyLock(t *testing.T) {
	g, task := newArtifactRequestValidationTest(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	g.modelRootDir = root
	path := filepath.Join(root, "family", "model")
	task.BaseModel.Spec.Storage.Path = &path
	release, acquired, err := g.acquireDirectArtifactPathLock(path)
	require.NoError(t, err)
	require.True(t, acquired)
	defer release()
	_, acquired, err = tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: root, ChildModelPath: filepath.Join(root, "family", "another")})
	require.NoError(t, err)
	require.False(t, acquired, "direct writers use the existing family lock namespace")
}
