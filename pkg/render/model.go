package render

import (
	"path/filepath"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils"
)

// UpdateVolumeMounts updates volume mounts for the container
func UpdateVolumeMounts(p *Piece, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	if container == nil {
		p.Log.Error(errors.New("container is nil"), "UpdateVolumeMounts: container is nil")
		return
	}

	if _, mount, ok := modelSource(p); ok && isvcutils.IsOriginalModelVolumeMountNecessary(objectMeta.Annotations) {
		isvcutils.AppendVolumeMount(container, &mount)
	}

	AppendOverlayVolumeMounts(p, p.Overlays, container)

	// Add fine-tuned serving volume mounts
	if p.FineTunedServing {
		defaultModelVolumeMount := corev1.VolumeMount{
			Name:      constants.ModelEmptyDirVolumeName,
			MountPath: constants.ModelDefaultMountPath,
		}
		isvcutils.AppendVolumeMountIfNotExist(container, &defaultModelVolumeMount)

		if isvcutils.IsCohereCommand1TFewFTServing(objectMeta) {
			// Update to have `base` sub-path in model volume mount for cohere tfew stacked serving case
			defaultModelVolumeMountWithSubPath := corev1.VolumeMount{
				Name:      constants.ModelEmptyDirVolumeName,
				MountPath: filepath.Join(constants.ModelDefaultMountPath, objectMeta.Annotations[constants.BaseModelFormat]),
				SubPath:   constants.BaseModelVolumeMountSubPath,
			}
			isvcutils.UpdateVolumeMount(container, &defaultModelVolumeMountWithSubPath)

			tfewFineTunedWeightVolumeMount := corev1.VolumeMount{
				Name:      constants.ModelEmptyDirVolumeName,
				MountPath: filepath.Join(constants.CohereTFewFineTunedWeightVolumeMountPath, objectMeta.Annotations[constants.BaseModelFormat]),
				ReadOnly:  true,
				SubPath:   constants.FineTunedWeightVolumeMountSubPath,
			}
			isvcutils.AppendVolumeMount(container, &tfewFineTunedWeightVolumeMount)
		}
	}
}

// UpdateEnvVariables updates environment variables for the container
func UpdateEnvVariables(p *Piece, isvc *v1beta1.InferenceService, container *corev1.Container, objectMeta *metav1.ObjectMeta) {
	if container == nil {
		p.Log.Error(errors.New("container is nil"), "UpdateEnvVariables: container is nil")
		return
	}

	if !p.FineTunedServing {
		// Base model serving - add MODEL_PATH env variable if necessary
		if modelPath, ok := modelPathEnvValue(p, objectMeta); ok {
			p.Log.V(1).Info("Base model serving - adding MODEL_PATH env variable if not provided", "inference service", isvc.Name, "namespace", isvc.Namespace)
			isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
				{Name: constants.ModelPathEnvVarKey, Value: modelPath},
			})
		}
		AppendOverlayEnvVars(p, p.Overlays, container)
	} else {
		// Fine-tuned serving - add vendor-specific environment variables
		if p.BaseModel != nil && p.BaseModel.Vendor != nil {
			if *p.BaseModel.Vendor == string(constants.Meta) {
				// Llama/Meta vendor specific env vars
				isvcutils.UpdateEnvVars(container, &corev1.EnvVar{
					Name: constants.ServedModelNameEnvVarKey,
					Value: filepath.Join(
						constants.LLamaVllmFTServingServedModelNamePrefix,
						objectMeta.Annotations[constants.FineTunedAdapterInjectionKey],
					),
				})
				isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
					{Name: constants.ModelPathEnvVarKey, Value: constants.ModelDefaultMountPath},
				})
			} else if *p.BaseModel.Vendor == string(constants.Cohere) {
				// Cohere vendor specific env vars
				if isvcutils.IsCohereCommand1TFewFTServing(objectMeta) {
					isvcutils.AppendEnvVarsIfNotExist(container, &[]corev1.EnvVar{
						{Name: constants.TFewWeightPathEnvVarKey, Value: constants.CohereTFewFineTunedWeightDefaultPath},
					})
				}
			}
		} else {
			p.Log.Info("Warning: no vendor given in base model spec - no env var added/updated")
		}
	}

	// append env var from runtime spec if it is specified.
	// runner container is user values, it takes precedence over runtime values.
	// if the env exists, update its value.
	// if the env does not exist, append it to the list.
	if p.SupportedModelFormat != nil && p.SupportedModelFormat.AcceleratorConfig != nil && p.AcceleratorClassName != "" {
		acceleratorConfig := p.SupportedModelFormat.GetAcceleratorConfig(p.AcceleratorClassName)
		if acceleratorConfig != nil {
			envOverride := acceleratorConfig.EnvironmentOverride
			for envName, envVar := range envOverride {
				isvcutils.UpdateEnvVars(container, &corev1.EnvVar{
					Name: envName, Value: envVar})
			}
		}
	}
}

// UpdatePodSpecVolumes updates pod spec with common volumes
func UpdatePodSpecVolumes(p *Piece, isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, objectMeta *metav1.ObjectMeta) {
	if volume, _, ok := modelSource(p); ok {
		podSpec.Volumes = append(podSpec.Volumes, volume)
	}

	AppendOverlayVolumes(p, p.Overlays, podSpec)

	// Add empty model directory volume if required for fine-tuned serving
	if isvcutils.IsEmptyModelDirVolumeRequired(objectMeta.Annotations) {
		emptyModelDirVolume := corev1.Volume{
			Name: constants.ModelEmptyDirVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				},
			},
		}
		podSpec.Volumes = utils.AppendVolumeIfNotExists(podSpec.Volumes, emptyModelDirVolume)
	}
}

// modelSource is the volume holding the primary model's bytes and its read-only
// mount: a claim mounted at the default model path under its sub-path, or a host
// path mounted at that same path. ok is false when the pod mounts nothing for
// the model (no model, no storage, no metadata, or a model with the Sharded
// distribution, which the pod does not mount).
func modelSource(p *Piece) (corev1.Volume, corev1.VolumeMount, bool) {
	if p.BaseModel == nil || isvcutils.IsShardedBaseModel(p.BaseModel) || p.BaseModel.Storage == nil || p.BaseModelMeta == nil {
		return corev1.Volume{}, corev1.VolumeMount{}, false
	}
	source, subPath, ok := storageSource(p.BaseModel.Storage)
	if !ok {
		return corev1.Volume{}, corev1.VolumeMount{}, false
	}
	mountPath := constants.ModelDefaultMountPath
	if source.HostPath != nil {
		mountPath = source.HostPath.Path
	}
	name := p.BaseModelMeta.Name
	return corev1.Volume{Name: name, VolumeSource: source},
		corev1.VolumeMount{Name: name, MountPath: mountPath, SubPath: subPath, ReadOnly: true},
		true
}

// storageSource reads where a storage spec keeps a model's bytes: a
// PersistentVolumeClaim, with the sub-path inside the claim, or a path on the
// node. ok is false when the spec names neither.
func storageSource(s *v1beta1.StorageSpec) (source corev1.VolumeSource, subPath string, ok bool) {
	if pvc := parsePVCStorage(s); pvc != nil {
		return corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.PVCName, ReadOnly: true}}, pvc.SubPath, true
	}
	if s != nil && s.Path != nil {
		return corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: *s.Path}}, "", true
	}
	return corev1.VolumeSource{}, "", false
}

func modelPathEnvValue(p *Piece, objectMeta *metav1.ObjectMeta) (string, bool) {
	if p == nil || p.BaseModel == nil || p.BaseModel.Storage == nil {
		return "", false
	}
	if isvcutils.IsShardedBaseModel(p.BaseModel) {
		if p.BaseModel.Storage.StorageUri == nil || *p.BaseModel.Storage.StorageUri == "" {
			return "", false
		}
		return *p.BaseModel.Storage.StorageUri, true
	}
	if objectMeta == nil || !isvcutils.IsOriginalModelVolumeMountNecessary(objectMeta.Annotations) {
		return "", false
	}
	if isPVCStorage(p.BaseModel.Storage) {
		return constants.ModelDefaultMountPath, true
	}
	if p.BaseModel.Storage.Path == nil || *p.BaseModel.Storage.Path == "" {
		return "", false
	}
	return *p.BaseModel.Storage.Path, true
}
