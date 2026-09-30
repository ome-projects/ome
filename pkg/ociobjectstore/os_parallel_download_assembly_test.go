package ociobjectstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssembleDownloadedPartCleansTemporaryFile(t *testing.T) {
	for _, test := range []struct {
		name      string
		wantError string
	}{
		{name: "success"},
		{name: "open failure", wantError: "failed to open temporary file"},
		{name: "seek failure", wantError: "failed to seek to offset"},
		{name: "copy failure", wantError: "failed to copy part"},
	} {
		t.Run(test.name, func(t *testing.T) {
			partPath := filepath.Join(t.TempDir(), "part.tmp")
			require.NoError(t, os.WriteFile(partPath, []byte("part"), 0600))
			target, err := os.CreateTemp(t.TempDir(), "assembly-*")
			require.NoError(t, err)
			defer target.Close()
			_, err = target.WriteString("head")
			require.NoError(t, err)

			switch test.name {
			case "open failure":
				// A dangling link cannot be opened, but its directory entry still
				// needs cleanup. Unlike chmod, this also fails when tests run as root.
				require.NoError(t, os.Remove(partPath))
				require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "missing"), partPath))
			case "seek failure":
				require.NoError(t, target.Close())
			case "copy failure":
				require.NoError(t, target.Close())
				target, err = os.Open(target.Name())
				require.NoError(t, err)
				defer target.Close()
			}

			store := &OCIOSDataStore{logger: &MockTestLogger{}}
			err = store.assembleDownloadedPart(target, &DownloadedPart{
				partNum: 1, offset: 4, tempFilePath: partPath,
			})
			if test.wantError == "" {
				require.NoError(t, err)
				content, readErr := os.ReadFile(target.Name())
				require.NoError(t, readErr)
				require.Equal(t, "headpart", string(content))
			} else {
				require.ErrorContains(t, err, test.wantError)
			}
			_, err = os.Lstat(partPath)
			require.ErrorIs(t, err, os.ErrNotExist, "every assembly exit must remove the current part")
		})
	}
}
