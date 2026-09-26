package modelagent

import "sigs.k8s.io/ome/pkg/constants"

// ArtifactResidencyAnnotation requests local artifact residency independently
// of Model CR lifetime. The requester is responsible for eviction eligibility.
const ArtifactResidencyAnnotation = constants.ModelArtifactResidencyAnnotation

func artifactEvictionRequested(task *GopherTask) bool {
	meta := taskModelMeta(task)
	return meta != nil && meta.Annotations[ArtifactResidencyAnnotation] == string(ModelStatusEvicted)
}

// A completed acknowledgement records history, not continuing path ownership.
// Anything retaining a cleanup receipt or artifact reference remains protected.
func completedArtifactEviction(entry ModelEntry) bool {
	return entry.Status == ModelStatusEvicted && entry.ModelUID != "" &&
		entry.DirectArtifactPendingEviction == nil && entry.HfArtifactPendingDeletion == nil &&
		entry.HfArtifactKey == "" && entry.Config == nil
}
