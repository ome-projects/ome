package components

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/render"
)

// MountedOverlaySummary lists the overlays that reach the pods, for status.
func MountedOverlaySummary(overlays []isvcutils.ResolvedOverlay) []v1beta1.MountedOverlay {
	return render.MountedOverlaySummary(overlays)
}

// SkippedOverlayReasons lists why each skipped overlay was left out.
func SkippedOverlayReasons(overlays []isvcutils.ResolvedOverlay) []string {
	return render.SkippedOverlayReasons(overlays)
}
