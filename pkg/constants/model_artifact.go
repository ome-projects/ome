package constants

import (
	"crypto/sha256"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
)

// GetModelArtifactRequestLabel binds local readiness to a Model CR instance.
// Hashing keeps arbitrary UIDs within the Kubernetes label-key limit.
func GetModelArtifactRequestLabel(uid types.UID) string {
	digest := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("ome.io/artifact-%x", digest[:24])
}
