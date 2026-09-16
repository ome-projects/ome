package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func specWithStorage(uri string, path *string) *v1beta1.BaseModelSpec {
	return &v1beta1.BaseModelSpec{
		Storage: &v1beta1.StorageSpec{
			StorageUri: &uri,
			Path:       path,
		},
	}
}

func TestValidateStageStorageAcceptsAbsoluteSourceAndPath(t *testing.T) {
	dest := "/mnt/data/models/qwen3-32b"

	err := ValidateStageStorage(specWithStorage("stage:///mnt/nfs-src/qwen/Qwen3-32B", &dest))

	assert.NoError(t, err)
}

// Without spec.storage.path the agent has nowhere to copy to, and the failure
// would only surface on the node.
func TestValidateStageStorageRejectsMissingPath(t *testing.T) {
	err := ValidateStageStorage(specWithStorage("stage:///mnt/nfs-src/qwen/Qwen3-32B", nil))

	assert.ErrorContains(t, err, "spec.storage.path")
}

func TestValidateStageStorageRejectsEmptyPath(t *testing.T) {
	empty := ""

	err := ValidateStageStorage(specWithStorage("stage:///mnt/nfs-src/qwen/Qwen3-32B", &empty))

	assert.ErrorContains(t, err, "spec.storage.path")
}

func TestValidateStageStorageRejectsRelativePath(t *testing.T) {
	relative := "models/qwen3-32b"

	err := ValidateStageStorage(specWithStorage("stage:///mnt/nfs-src/qwen/Qwen3-32B", &relative))

	assert.ErrorContains(t, err, "absolute")
}

func TestValidateStageStorageRejectsInvalidSourceURI(t *testing.T) {
	dest := "/mnt/data/models/qwen3-32b"

	err := ValidateStageStorage(specWithStorage("stage://relative/source", &dest))

	assert.ErrorContains(t, err, "must be an absolute path")
}

// Destination inside source would make the agent copy the tree into itself.
func TestValidateStageStorageRejectsPathInsideSource(t *testing.T) {
	dest := "/mnt/nfs-src/qwen/Qwen3-32B/copy"

	err := ValidateStageStorage(specWithStorage("stage:///mnt/nfs-src/qwen/Qwen3-32B", &dest))

	assert.ErrorContains(t, err, "must not be inside")
}

func TestValidateStageStorageIgnoresOtherProtocols(t *testing.T) {
	assert.NoError(t, ValidateStageStorage(specWithStorage("local:///mnt/data/models/x", nil)))
	assert.NoError(t, ValidateStageStorage(specWithStorage("pvc://claim/sub", nil)))
	assert.NoError(t, ValidateStageStorage(nil))
}
