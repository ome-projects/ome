package modelagent

import "sigs.k8s.io/ome/pkg/constants"

// ArtifactResidencyAnnotation requests local artifact residency independently
// of Model CR lifetime. The requester is responsible for eviction eligibility.
const ArtifactResidencyAnnotation = constants.ModelArtifactResidencyAnnotation

func artifactEvictionRequested(task *GopherTask) bool {
	meta := taskModelMeta(task)
	return meta != nil && meta.Annotations[ArtifactResidencyAnnotation] == string(ModelStatusEvicted)
}
