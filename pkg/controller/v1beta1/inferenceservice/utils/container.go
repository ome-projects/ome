package utils

import (
	"slices"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

func AppendVolumeMount(container *v1.Container, volumeMount *v1.VolumeMount) {
	container.VolumeMounts = append(container.VolumeMounts, *volumeMount)
}

// UpdateVolumeMount sets the path, sub-path and read-only flag of the mount
// with the same name, or appends the mount when the container has none.
func UpdateVolumeMount(container *v1.Container, volumeMount *v1.VolumeMount) {
	if volumeMount == nil {
		return
	}
	container.VolumeMounts = upsertByName(container.VolumeMounts, volumeMountName, *volumeMount, func(existing *v1.VolumeMount) {
		existing.MountPath = volumeMount.MountPath
		existing.SubPath = volumeMount.SubPath
		existing.ReadOnly = volumeMount.ReadOnly
	})
}

// AppendVolumeMountIfNotExist appends the mount unless one with the same name is there.
func AppendVolumeMountIfNotExist(container *v1.Container, volumeMount *v1.VolumeMount) {
	container.VolumeMounts = upsertByName(container.VolumeMounts, volumeMountName, *volumeMount, nil)
}

// AppendEnvVarsIfNotExist appends each variable unless one with the same name is there.
func AppendEnvVarsIfNotExist(container *v1.Container, envVars *[]v1.EnvVar) {
	if envVars == nil {
		return
	}
	for _, envVar := range *envVars {
		container.Env = upsertByName(container.Env, envVarName, envVar, nil)
	}
}

// UpdateEnvVars replaces the variable with the same name, or appends it.
func UpdateEnvVars(container *v1.Container, envVar *v1.EnvVar) {
	container.Env = upsertByName(container.Env, envVarName, *envVar, func(existing *v1.EnvVar) {
		*existing = *envVar
	})
}

func volumeMountName(mount v1.VolumeMount) string { return mount.Name }

func envVarName(envVar v1.EnvVar) string { return envVar.Name }

// upsertByName hands the first item with the same name to onFound, or appends
// item when there is none. A nil onFound keeps the item that is there as it is.
func upsertByName[T any](items []T, nameOf func(T) string, item T, onFound func(existing *T)) []T {
	name := nameOf(item)
	i := slices.IndexFunc(items, func(candidate T) bool { return nameOf(candidate) == name })
	if i < 0 {
		return append(items, item)
	}
	if onFound != nil {
		onFound(&items[i])
	}
	return items
}

// GetGpuCountFromContainer extracts the accelerator count from container
// resources: it walks acceleratorResources in order and returns the value of
// the first name present, checking that name's Limits before its Requests.
//
// acceleratorResources is the operator-configured list of extended resource
// names that count as an accelerator (inferenceservice-config ConfigMap's
// "acceleratorResources" key — see
// controllerconfig.InferenceServicesConfig.AcceleratorResourceNames). There is
// NO in-code default list: an empty/nil acceleratorResources means the config
// is absent, and this falls back to recognizing only
// constants.NvidiaGPUResourceType, so an unconfigured cluster keeps
// NVIDIA-only counting.
func GetGpuCountFromContainer(container *v1.Container, acceleratorResources []string) int {
	if container == nil {
		return 0
	}
	names := acceleratorResources
	if len(names) == 0 {
		names = []string{constants.NvidiaGPUResourceType}
	}

	for _, name := range names {
		resourceName := v1.ResourceName(name)
		if quantity, ok := container.Resources.Limits[resourceName]; ok {
			return int(quantity.Value())
		}
		if quantity, ok := container.Resources.Requests[resourceName]; ok {
			return int(quantity.Value())
		}
	}
	return 0
}
