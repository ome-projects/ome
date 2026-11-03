package pod

import (
	"fmt"
	"slices"

	v1 "k8s.io/api/core/v1"
)

// findContainer returns the container with the name, or nil.
func findContainer(containers []v1.Container, name string) *v1.Container {
	i := slices.IndexFunc(containers, func(c v1.Container) bool { return c.Name == name })
	if i < 0 {
		return nil
	}
	return &containers[i]
}

// targetContainerName is the container an injector edits: the one the
// annotation names, else the default.
func targetContainerName(pod *v1.Pod, annotationKey string) string {
	if name, ok := pod.ObjectMeta.Annotations[annotationKey]; ok && name != "" {
		return name
	}
	return DefaultContainerName
}

func hasVolume(pod *v1.Pod, name string) bool {
	return slices.ContainsFunc(pod.Spec.Volumes, func(v v1.Volume) bool { return v.Name == name })
}

func hasVolumeMount(container *v1.Container, name string) bool {
	return slices.ContainsFunc(container.VolumeMounts, func(m v1.VolumeMount) bool { return m.Name == name })
}

// ensureVolume adds the volume unless the pod has one of that name.
func ensureVolume(pod *v1.Pod, volume v1.Volume) {
	if !hasVolume(pod, volume.Name) {
		pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
	}
}

// ensureVolumeMount adds the mount unless the container has one of that name.
func ensureVolumeMount(container *v1.Container, mount v1.VolumeMount) {
	if !hasVolumeMount(container, mount.Name) {
		container.VolumeMounts = append(container.VolumeMounts, mount)
	}
}

func hasCapability(capabilities []v1.Capability, capability v1.Capability) bool {
	return slices.Contains(capabilities, capability)
}

// lookupProfile reads the profile the annotation names. A missing or empty
// annotation returns nil; a name outside the table is an error.
func lookupProfile[T any](pod *v1.Pod, annotationKey string, profiles map[string]T, kind string) (*T, error) {
	name, ok := pod.ObjectMeta.Annotations[annotationKey]
	if !ok || name == "" {
		return nil, nil
	}
	profile, ok := profiles[name]
	if !ok {
		return nil, fmt.Errorf("unknown %s profile: %s", kind, name)
	}
	return &profile, nil
}
