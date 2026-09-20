package modelagent

import (
	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	"sigs.k8s.io/ome/pkg/constants"
)

func filterInternalArtifactObjectSummaries(objects []objectstorage.ObjectSummary) []objectstorage.ObjectSummary {
	filtered := make([]objectstorage.ObjectSummary, 0, len(objects))
	for _, object := range objects {
		if object.Name != nil && isInternalArtifactObjectName(*object.Name) {
			continue
		}
		filtered = append(filtered, object)
	}
	return filtered
}

func isInternalArtifactObjectName(objectName string) bool {
	return constants.IsInternalArtifactObjectName(objectName)
}
