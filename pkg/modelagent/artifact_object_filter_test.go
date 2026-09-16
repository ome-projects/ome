package modelagent

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestFilterInternalArtifactObjectSummaries(t *testing.T) {
	names := []string{
		"prefix/config.json", constants.ArtifactCompleteMarkerFileName,
		"prefix/" + constants.ArtifactCompleteMarkerFileName,
		constants.ArtifactUploadLockFileName, "prefix/" + constants.ArtifactUploadLockFileName,
		"prefix/not-" + constants.ArtifactCompleteMarkerFileName, "prefix/model.safetensors",
	}
	objects := []objectstorage.ObjectSummary{{}}
	for i := range names {
		objects = append(objects, objectstorage.ObjectSummary{Name: &names[i]})
	}
	before := append([]objectstorage.ObjectSummary(nil), objects...)
	filtered := filterInternalArtifactObjectSummaries(objects)
	require.Equal(t, []objectstorage.ObjectSummary{objects[1], objects[6], objects[7]}, filtered)
	require.Equal(t, before, objects, "filter must not overwrite the source slice")
	filtered[0] = objectstorage.ObjectSummary{}
	require.Equal(t, before, objects, "filtered slice must not alias source storage")
}

func TestFilterInternalArtifactObjectSummariesEmpty(t *testing.T) {
	marker := constants.ArtifactCompleteMarkerFileName
	for _, objects := range [][]objectstorage.ObjectSummary{nil, {}, {{}, {Name: &marker}}} {
		require.Empty(t, filterInternalArtifactObjectSummaries(objects))
	}
}
