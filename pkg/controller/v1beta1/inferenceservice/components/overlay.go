package components

import (
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/render"
)

// The overlay helpers operate on the embedded render.Piece. These forwarders
// keep the component-level names for callers holding a *BaseComponentFields,
// which may be nil.

func AppendOverlayVolumes(b *BaseComponentFields, overlays []isvcutils.ResolvedOverlay, podSpec *corev1.PodSpec) {
	render.AppendOverlayVolumes(pieceOf(b), overlays, podSpec)
}

func AppendOverlayVolumeMounts(b *BaseComponentFields, overlays []isvcutils.ResolvedOverlay, container *corev1.Container) {
	render.AppendOverlayVolumeMounts(pieceOf(b), overlays, container)
}

func AppendOverlayEnvVars(b *BaseComponentFields, overlays []isvcutils.ResolvedOverlay, container *corev1.Container) {
	render.AppendOverlayEnvVars(pieceOf(b), overlays, container)
}

// AnyOverlayIsSharded reports whether any mounted overlay is Sharded.
func AnyOverlayIsSharded(overlays []isvcutils.ResolvedOverlay) bool {
	return render.AnyOverlayIsSharded(overlays)
}

// MountedOverlaySummary lists the overlays that reach the pods, for status.
func MountedOverlaySummary(overlays []isvcutils.ResolvedOverlay) []v1beta1.MountedOverlay {
	return render.MountedOverlaySummary(overlays)
}

// SkippedOverlayReasons lists why each skipped overlay was left out.
func SkippedOverlayReasons(overlays []isvcutils.ResolvedOverlay) []string {
	return render.SkippedOverlayReasons(overlays)
}
