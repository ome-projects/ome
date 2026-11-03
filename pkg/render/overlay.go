package render

import (
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils"
)

func overlayVolumeName(modelName string) string {
	return "model-overlay-" + modelName
}

func AppendOverlayVolumes(p *Piece, overlays []isvcutils.ResolvedOverlay, podSpec *corev1.PodSpec) {
	if podSpec == nil {
		return
	}
	for _, ov := range overlays {
		if vol, _, ok := overlaySource(ov); ok {
			podSpec.Volumes = utils.AppendVolumeIfNotExists(podSpec.Volumes, vol)
		}
	}
}

func AppendOverlayVolumeMounts(p *Piece, overlays []isvcutils.ResolvedOverlay, container *corev1.Container) {
	if container == nil {
		return
	}
	for _, ov := range overlays {
		if _, vm, ok := overlaySource(ov); ok {
			isvcutils.AppendVolumeMount(container, &vm)
		}
	}
}

func AppendOverlayEnvVars(p *Piece, overlays []isvcutils.ResolvedOverlay, container *corev1.Container) {
	if container == nil {
		return
	}
	envs := make([]corev1.EnvVar, 0, len(overlays))
	for _, ov := range overlays {
		if path, ok := overlayEffectivePath(ov); ok {
			envs = append(envs, corev1.EnvVar{
				Name:  isvcutils.OverlayEnvVarName(ov.Ref.Name),
				Value: path,
			})
		}
	}
	if len(envs) > 0 {
		isvcutils.AppendEnvVarsIfNotExist(container, &envs)
	}
}

// AnyOverlayIsSharded reports whether any resolved overlay is a Sharded
// model. A Sharded overlay needs the model cache client even when the
// primary model is not Sharded, because its data is fetched at runtime.
func AnyOverlayIsSharded(overlays []isvcutils.ResolvedOverlay) bool {
	for _, ov := range overlays {
		if !ov.Skipped() && isvcutils.IsShardedBaseModel(ov.Spec) {
			return true
		}
	}
	return false
}

func MountedOverlaySummary(overlays []isvcutils.ResolvedOverlay) []v1beta1.MountedOverlay {
	out := make([]v1beta1.MountedOverlay, 0, len(overlays))
	for _, ov := range overlays {
		if ov.Skipped() {
			continue
		}
		entry := v1beta1.MountedOverlay{
			Name:         ov.Ref.Name,
			EnvVar:       isvcutils.OverlayEnvVarName(ov.Ref.Name),
			Distribution: overlayDistributionString(ov.Spec),
		}
		if _, vm, ok := overlaySource(ov); ok {
			entry.MountPath = vm.MountPath
		}
		out = append(out, entry)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func SkippedOverlayReasons(overlays []isvcutils.ResolvedOverlay) []string {
	var out []string
	for _, ov := range overlays {
		if ov.Skipped() {
			out = append(out, ov.SkipReason)
		}
	}
	return out
}

// overlayEffectivePath: PVC/PerNode → mount path; Sharded → storage URI.
// Sharded data is fetched at runtime; PVC/PerNode is mounted on disk.
func overlayEffectivePath(ov isvcutils.ResolvedOverlay) (string, bool) {
	if ov.Skipped() || ov.Spec == nil || ov.Spec.Storage == nil {
		return "", false
	}
	if isvcutils.IsShardedBaseModel(ov.Spec) {
		if ov.Spec.Storage.StorageUri == nil || *ov.Spec.Storage.StorageUri == "" {
			return "", false
		}
		return *ov.Spec.Storage.StorageUri, true
	}
	return isvcutils.OverlayMountPath(ov.Ref.Name), true
}

// overlaySource is the volume holding an overlay's bytes and its read-only
// mount under the overlay mount prefix: a claim with its sub-path, or a host
// path. ok is false for a skipped overlay, one without storage, and one whose
// bytes the runtime fetches by storage URI instead of a mount (see
// overlayEffectivePath).
func overlaySource(ov isvcutils.ResolvedOverlay) (corev1.Volume, corev1.VolumeMount, bool) {
	if ov.Skipped() || ov.Spec.Storage == nil || isvcutils.IsShardedBaseModel(ov.Spec) {
		return corev1.Volume{}, corev1.VolumeMount{}, false
	}
	source, subPath, ok := storageSource(ov.Spec.Storage)
	if !ok {
		return corev1.Volume{}, corev1.VolumeMount{}, false
	}
	name := overlayVolumeName(ov.Ref.Name)
	return corev1.Volume{Name: name, VolumeSource: source},
		corev1.VolumeMount{Name: name, MountPath: isvcutils.OverlayMountPath(ov.Ref.Name), SubPath: subPath, ReadOnly: true},
		true
}

func overlayDistributionString(spec *v1beta1.BaseModelSpec) string {
	if spec == nil || spec.Distribution == nil {
		return string(v1beta1.DistributionPerNode)
	}
	return string(*spec.Distribution)
}
