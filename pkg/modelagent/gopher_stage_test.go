package modelagent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func stageSpec(path *string) v1beta1.BaseModelSpec {
	uri := "stage:///mnt/nfs-src/qwen/Qwen3-32B"
	return v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			StorageUri: &uri,
			Path:       path,
		},
	}
}

func TestStageDestPathUsesStoragePath(t *testing.T) {
	g := &Gopher{logger: zap.NewNop().Sugar(), modelRootDir: "/mnt/data/models"}
	path := "/mnt/data/models/qwen3-32b"

	got, err := g.stageDestPath(stageSpec(&path))

	require.NoError(t, err)
	assert.Equal(t, "/mnt/data/models/qwen3-32b", got)
}

// getDestPath falls back to modelRootDir + storageUri when Path is unset, which
// for stage:// would build a directory literally named after the URI.
func TestStageDestPathRejectsMissingPath(t *testing.T) {
	g := &Gopher{logger: zap.NewNop().Sugar(), modelRootDir: "/mnt/data/models"}

	_, err := g.stageDestPath(stageSpec(nil))

	assert.ErrorContains(t, err, "requires spec.storage.path")
}

func TestStageDestPathRejectsEmptyPath(t *testing.T) {
	g := &Gopher{logger: zap.NewNop().Sugar(), modelRootDir: "/mnt/data/models"}
	empty := ""

	_, err := g.stageDestPath(stageSpec(&empty))

	assert.ErrorContains(t, err, "requires spec.storage.path")
}

func TestStageDestPathRejectsRelativePath(t *testing.T) {
	g := &Gopher{logger: zap.NewNop().Sugar(), modelRootDir: "/mnt/data/models"}
	relative := "models/qwen3-32b"

	_, err := g.stageDestPath(stageSpec(&relative))

	assert.ErrorContains(t, err, "must be an absolute path")
}
