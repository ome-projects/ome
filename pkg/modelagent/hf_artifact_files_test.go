package modelagent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHfArtifactFilesystemPathsRequireExpectedParent(t *testing.T) {
	for _, changed := range []string{"none", "key", "path"} {
		t.Run(changed, func(t *testing.T) {
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			parent := input.Parent
			switch changed {
			case "key":
				input.Parent.Key += ".other"
			case "path":
				input.Parent.LocalPath = canonicalHfArtifactPath(filepath.Join(input.ModelStoreRoot, "other-store", "child"), parent.Identity)
			}

			err := input.validateFilesystemPaths(parent)

			if changed == "none" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "shared artifact parent path changed")
			}
		})
	}
}

func TestHfArtifactFilesHasChildren(t *testing.T) {
	for _, tt := range []struct {
		name          string
		relativeLink  bool
		parentMissing bool
		otherParent   bool
		insideParent  bool
		wantChildren  bool
	}{
		{name: "absolute child link", wantChildren: true},
		{name: "relative child link", relativeLink: true, wantChildren: true},
		{name: "dangling child link", relativeLink: true, parentMissing: true, wantChildren: true},
		{name: "link to another parent", otherParent: true},
		{name: "link inside parent is not a child", insideParent: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			parentPath := filepath.Join(root, "_artifacts", "Qwen", "Qwen3-8B", testHFCommitSHA)
			if !tt.parentMissing {
				require.NoError(t, os.MkdirAll(parentPath, 0o755))
			}
			childPath := filepath.Join(root, "nested", "model-1")
			if tt.insideParent {
				childPath = filepath.Join(parentPath, "internal-link")
			}
			require.NoError(t, os.MkdirAll(filepath.Dir(childPath), 0o755))
			target := parentPath
			if tt.otherParent {
				target = filepath.Join(root, "_artifacts", "another-parent")
			}
			if tt.relativeLink {
				var err error
				target, err = filepath.Rel(filepath.Dir(childPath), target)
				require.NoError(t, err)
			}
			require.NoError(t, os.Symlink(target, childPath))

			files := hfArtifactFiles{}
			hasChildren, err := files.HasChildren(parentPath, root)

			require.NoError(t, err)
			assert.Equal(t, tt.wantChildren, hasChildren)
		})
	}
}

func TestHfArtifactFilesHasChildrenRejectsNonDirectoryRoot(t *testing.T) {
	for _, rootType := range []string{"file", "symlink"} {
		t.Run(rootType, func(t *testing.T) {
			root := t.TempDir()
			parentPath := filepath.Join(root, "parent")
			require.NoError(t, os.MkdirAll(parentPath, 0o755))
			require.NoError(t, os.Symlink(parentPath, filepath.Join(root, "child")))
			scanRoot := filepath.Join(t.TempDir(), "scan-root")
			if rootType == "symlink" {
				require.NoError(t, os.Symlink(root, scanRoot))
			} else {
				require.NoError(t, os.WriteFile(scanRoot, nil, 0o644))
			}

			_, err := (hfArtifactFiles{}).HasChildren(parentPath, scanRoot)

			require.Error(t, err, "an unscanned root must not prove that no children exist")
		})
	}
}

func TestHfArtifactFilesHasChildrenReturnsSymlinkReadError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read symlinks without directory search permission")
	}
	root := t.TempDir()
	parentPath := filepath.Join(root, "parent")
	directory := filepath.Join(root, "children")
	require.NoError(t, os.MkdirAll(directory, 0o755))
	childPath := filepath.Join(directory, "child")
	require.NoError(t, os.Symlink(parentPath, childPath))
	require.NoError(t, os.Chmod(directory, 0o400))
	t.Cleanup(func() { require.NoError(t, os.Chmod(directory, 0o755)) })
	if _, err := os.Readlink(childPath); err == nil {
		t.Skip("filesystem does not enforce directory search permission")
	}

	_, err := (hfArtifactFiles{}).HasChildren(parentPath, root)

	require.Error(t, err, "an unreadable link must not be treated as an unrelated child")
}

func TestHfArtifactScanFindsUnknownLinkInOtherArtifact(t *testing.T) {
	root := t.TempDir()
	input := testHfArtifactTaskInput(t, root, "child")
	other := filepath.Join(root, "_artifacts", "other", "old-snapshot")
	require.NoError(t, os.MkdirAll(other, 0o755))
	require.NoError(t, os.Symlink(input.Parent.LocalPath, filepath.Join(other, "unknown-child")))
	found, err := (hfArtifactFiles{}).HasChildren(input.Parent.LocalPath, root)
	require.NoError(t, err)
	assert.True(t, found, "only the actual parent's contents may be skipped")
}
