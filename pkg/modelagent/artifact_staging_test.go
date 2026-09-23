package modelagent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArtifactStagingCleanupPreservesActiveDownloads(t *testing.T) {
	g, _, modelPath := newDirectArtifactTestModel(t)
	store := filepath.Join(g.modelRootDir, directArtifactStagingDirectory, directArtifactDownloadStages)
	active := filepath.Join(store, strings.Repeat("a", 64))
	abandoned := filepath.Join(store, strings.Repeat("b", 64))
	unknown := filepath.Join(store, "unrecognized")
	legacy := filepath.Join(g.modelRootDir, directArtifactStagingDirectory, strings.Repeat("c", 64))
	for _, path := range []string{active, abandoned, unknown, legacy} {
		require.NoError(t, os.MkdirAll(path, 0700))
		require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("keep until unlocked"), 0600))
	}
	lock, acquired, err := tryHfArtifactFileLock(g.modelRootDir, filepath.Join(g.modelRootDir, hfArtifactLockDirectory), "staging:"+active)
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	require.NoError(t, g.cleanupArtifactStaging())
	require.NoDirExists(t, abandoned)
	for _, path := range []string{active, unknown, legacy, modelPath} {
		require.FileExists(t, filepath.Join(path, "weights"))
	}
	require.NoError(t, lock.Close())
	require.NoError(t, g.cleanupArtifactStaging())
	require.NoDirExists(t, active, "a released stage can be reclaimed after a crashed attempt")
}

func TestArtifactStagingCleanupDoesNotFollowSymlinks(t *testing.T) {
	for _, ancestor := range []bool{false, true} {
		t.Run(map[bool]string{false: "stage", true: "store"}[ancestor], func(t *testing.T) {
			g, _, _ := newDirectArtifactTestModel(t)
			store := filepath.Join(g.modelRootDir, directArtifactStagingDirectory, directArtifactDownloadStages)
			outside := t.TempDir()
			protected := filepath.Join(outside, "weights")
			require.NoError(t, os.WriteFile(protected, []byte("keep"), 0600))
			link := filepath.Join(store, strings.Repeat("d", 64))
			if ancestor {
				link = store
			}
			require.NoError(t, os.MkdirAll(filepath.Dir(link), 0700))
			require.NoError(t, os.Symlink(outside, link))
			err := g.cleanupArtifactStaging()
			if ancestor {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.FileExists(t, protected)
		})
	}
}
