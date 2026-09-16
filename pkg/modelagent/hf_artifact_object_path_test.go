package modelagent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestHfArtifactObjectPathLiteralPrefix(t *testing.T) {
	parent := t.TempDir()
	for _, tc := range []struct {
		name, prefix, object, relative string
	}{
		{"root prefix", "", "config.json", "config.json"},
		{"directory", "models/Org/Model/ABC", "models/Org/Model/ABC/config.json", "config.json"},
		{"trailing slash", "models/Org/Model/ABC/", "models/Org/Model/ABC/sub/file", "sub/file"},
		{"percent is literal", "prefix/", "prefix/%2e%2e/%2Fweights", "%2e%2e/%2Fweights"},
		{"remote prefix is not cleaned", "literal//prefix/", "literal//prefix/file", "file"},
		{"spaces are literal", "prefix/", "prefix/sub dir/weights ", "sub dir/weights "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hfArtifactObjectPath(parent, tc.prefix, tc.object)
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(parent, filepath.FromSlash(tc.relative)), got)
			_, err = os.Lstat(got)
			assert.True(t, os.IsNotExist(err), "planning must not create paths")
		})
	}
}

func TestHfArtifactObjectPathRejectsUnsafeKeys(t *testing.T) {
	parent := t.TempDir()
	for _, tc := range []struct{ prefix, object string }{
		{"prefix", "prefix-collision/file"},
		{"prefix/", "other/file"},
		{"Prefix/", "prefix/file"},
		{"Model/ABC/", "Model/abc/file"},
		{"prefix/", "prefix/"},
		{"prefix", "prefix"},
		{"", ""},
		{"", "/absolute"},
		{"prefix/", "prefix//absolute"},
		{"prefix/", "prefix/../escape"},
		{"prefix/", "prefix/a/../../escape"},
		{"prefix/", "prefix/./weights"},
		{"prefix/", "prefix/a//weights"},
		{"prefix/", "prefix/a/"},
		{"prefix/", "prefix/a\\weights"},
		{"prefix/", "prefix/a\x00weights"},
		{"literal//prefix/", "literal/prefix/file"},
		{"prefix/", "prefix/" + constants.HfArtifactReadyMarkerFileName},
		{"prefix/", "prefix/nested/" + constants.HfArtifactReadyMarkerFileName},
		{"prefix/", "prefix/" + constants.HfArtifactReadyMarkerFileName + "/file"},
		{"prefix/", "prefix/" + constants.ArtifactCompleteMarkerFileName},
		{"prefix/", "prefix/nested/" + constants.ArtifactUploadLockFileName},
	} {
		t.Run(tc.object, func(t *testing.T) {
			got, err := hfArtifactObjectPath(parent, tc.prefix, tc.object)
			assert.Error(t, err)
			assert.Empty(t, got)
		})
	}
}

func TestHfArtifactObjectPathRejectsSymlinks(t *testing.T) {
	for _, kind := range []string{"leaf", "dangling leaf", "directory", "dangling directory", "parent", "dangling parent"} {
		t.Run(kind, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			parent := filepath.Join(root, "parent")
			require.NoError(t, os.Mkdir(parent, 0o755))
			target := filepath.Join(outside, "untouched")
			require.NoError(t, os.WriteFile(target, []byte("untouched"), 0o600))
			object := "file"
			switch kind {
			case "leaf":
				require.NoError(t, os.Symlink(target, filepath.Join(parent, object)))
			case "dangling leaf":
				require.NoError(t, os.Symlink(filepath.Join(outside, "missing"), filepath.Join(parent, object)))
			case "directory":
				require.NoError(t, os.Symlink(outside, filepath.Join(parent, "nested")))
				object = "nested/untouched"
			case "dangling directory":
				require.NoError(t, os.Symlink(filepath.Join(outside, "missing"), filepath.Join(parent, "nested")))
				object = "nested/file"
			case "parent":
				parent = filepath.Join(root, "link")
				require.NoError(t, os.Symlink(outside, parent))
			case "dangling parent":
				parent = filepath.Join(root, "link")
				require.NoError(t, os.Symlink(filepath.Join(outside, "missing"), parent))
			}
			_, err := hfArtifactObjectPath(parent, "prefix/", "prefix/"+object)
			require.Error(t, err)
			data, err := os.ReadFile(target)
			require.NoError(t, err)
			assert.Equal(t, "untouched", string(data))
		})
	}
}

func TestHfArtifactObjectPathParentAndLeafTypes(t *testing.T) {
	root := t.TempDir()
	missingParent := filepath.Join(root, "missing", "parent")
	got, err := hfArtifactObjectPath(missingParent, "p/", "p/nested/file")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(missingParent, "nested", "file"), got)
	assert.NoDirExists(t, filepath.Join(root, "missing"))
	file := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(file, []byte("data"), 0o600))
	got, err = hfArtifactObjectPath(root, "p/", "p/file")
	require.NoError(t, err)
	assert.Equal(t, file, got, "existing regular files can be validated or repaired")
	for _, parent := range []string{"", "/", "relative", root + "/../parent", file} {
		_, err := hfArtifactObjectPath(parent, "p/", "p/new-file")
		assert.Error(t, err)
	}
	require.NoError(t, os.Mkdir(filepath.Join(root, "directory"), 0o755))
	_, err = hfArtifactObjectPath(root, "p/", "p/directory")
	assert.Error(t, err, "object leaves must be files")
	_, err = hfArtifactObjectPath(root, "p/", "p/file/nested")
	assert.Error(t, err, "a file cannot be a directory ancestor")
}
