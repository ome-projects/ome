package pod

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	v1 "k8s.io/api/core/v1"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/constants"
)

const (
	// RDMAConfigKeyName is the inferenceservice-config key that holds the
	// RDMA profile table.
	RDMAConfigKeyName = "rdma"
	// DshmVolumeName is the name of the /dev/shm volume
	DshmVolumeName = "dshm"
	// DefaultContainerName is the default container name to inject into if not specified
	DefaultContainerName = "ome-container"
	// MultusNetworksAnnotation is the pod annotation Multus reads to attach
	// extra NetworkAttachmentDefinitions. Value is a JSON array of
	// {name, namespace} objects.
	MultusNetworksAnnotation = "k8s.v1.cni.cncf.io/networks"
)

// RDMAConfig is the rdma block of the inferenceservice-config ConfigMap: the
// profiles a pod may name through ome.io/rdma-profile and the one applied
// when it names none. Neither the binary nor the chart carries a profile;
// the cluster's own values file describes its fabric.
type RDMAConfig struct {
	DefaultProfile string                 `json:"defaultProfile"`
	Profiles       map[string]RDMAProfile `json:"profiles"`
}

// hasProfiles reports whether the table names at least one profile. An
// absent block and a block without profiles both inject nothing.
func (c *RDMAConfig) hasProfiles() bool {
	return c != nil && len(c.Profiles) > 0
}

// NetworkAttachment is a Multus network reference. Serialized into
// k8s.v1.cni.cncf.io/networks per the multus-cni JSON schema.
type NetworkAttachment struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// RDMAProfile is what one profile adds to a pod: environment for the
// collective and transfer libraries, the device and shared-memory volumes
// with their mounts, the capabilities RDMA verbs need, and the Multus
// networks to attach. Values already on the container win over the profile.
type RDMAProfile struct {
	EnvVars         map[string]string   `json:"env,omitempty"`
	VolumeMounts    []v1.VolumeMount    `json:"volumeMounts,omitempty"`
	Volumes         []v1.Volume         `json:"volumes,omitempty"`
	SecurityContext *v1.SecurityContext `json:"securityContext,omitempty"`
	// Networks are merged into the pod's k8s.v1.cni.cncf.io/networks
	// annotation; dedup is by (name, namespace).
	Networks []NetworkAttachment `json:"networks,omitempty"`
}

// newRDMAConfig parses the rdma block. An absent block is nil: pods that
// ask for RDMA injection then get nothing.
func newRDMAConfig(configMap *v1.ConfigMap) (*RDMAConfig, error) {
	raw, ok := configMap.Data[RDMAConfigKeyName]
	if !ok {
		return nil, nil
	}
	config := &RDMAConfig{}
	if err := json.Unmarshal([]byte(raw), config); err != nil {
		return nil, fmt.Errorf("unable to unmarshal %v json string: %w", RDMAConfigKeyName, err)
	}
	return config, nil
}

// injectRDMA applies an RDMA profile (env / volumes / Multus attachments)
// when the ome.io/rdma-auto-inject=true annotation is set. The profile is
// the one ome.io/rdma-profile names, else the table's default; a table with
// no profiles injects nothing.
func injectRDMA(pod *v1.Pod, table *RDMAConfig) error {
	if autoInject, ok := pod.ObjectMeta.Annotations[constants.RDMAAutoInjectAnnotationKey]; !ok || autoInject != "true" {
		return nil
	}
	if !table.hasProfiles() {
		return nil
	}
	profileName := table.DefaultProfile
	if profile, ok := pod.ObjectMeta.Annotations[constants.RDMAProfileAnnotationKey]; ok && profile != "" {
		profileName = profile
	}
	if profileName == "" {
		return fmt.Errorf("no RDMA profile: the pod names none and %s sets no defaultProfile", RDMAConfigKeyName)
	}
	profile, ok := table.Profiles[profileName]
	if !ok {
		return fmt.Errorf("unknown RDMA profile: %s", profileName)
	}
	return injectRDMAConfig(pod, profile)
}

func injectRDMAConfig(pod *v1.Pod, profile RDMAProfile) error {
	containerName := targetContainerName(pod, constants.RDMAContainerNameAnnotationKey)
	target := findContainer(pod.Spec.Containers, containerName)
	if target == nil {
		// Quiet log: the runtime YAML names a custom container that
		// the rendered pod doesn't have; operators see this when an
		// init container mistakenly carries the annotation.
		ctrllog.Log.WithName("rdma-injector").V(1).Info("RDMA injection skipped: container not found",
			"container", containerName,
			"pod", pod.Name,
			"namespace", pod.Namespace)
		return nil
	}

	injectVolumes(pod, profile.Volumes)
	if err := injectNetworks(pod, profile.Networks); err != nil {
		return err
	}
	injectContainerConfig(target, profile)
	return nil
}

// injectNetworks merges `networks` into the pod's
// k8s.v1.cni.cncf.io/networks annotation. Existing entries are preserved;
// dedup is by (name, namespace). Returns an error if the existing
// annotation is malformed (rather than silently clobbering it).
func injectNetworks(pod *v1.Pod, networks []NetworkAttachment) error {
	if len(networks) == 0 {
		return nil
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}

	merged := []NetworkAttachment{}
	if raw, ok := pod.Annotations[MultusNetworksAnnotation]; ok && raw != "" {
		if err := json.Unmarshal([]byte(raw), &merged); err != nil {
			return fmt.Errorf("rdma-injector: %s annotation is not a valid JSON array: %w",
				MultusNetworksAnnotation, err)
		}
	}
	seen := map[string]struct{}{}
	for _, n := range merged {
		seen[n.Name+"/"+n.Namespace] = struct{}{}
	}
	for _, n := range networks {
		key := n.Name + "/" + n.Namespace
		if _, ok := seen[key]; ok {
			continue
		}
		merged = append(merged, n)
		seen[key] = struct{}{}
	}

	out, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("rdma-injector: marshal %s: %w", MultusNetworksAnnotation, err)
	}
	pod.Annotations[MultusNetworksAnnotation] = string(out)
	return nil
}

// injectContainerConfig applies the profile's container settings to the
// target; values already on the container win.
func injectContainerConfig(container *v1.Container, profile RDMAProfile) {
	mergeEnv(container, profile.EnvVars)
	mergeMounts(container, profile.VolumeMounts)
	mergeSecurityContext(container, profile.SecurityContext)
}

// mergeEnv appends the profile's variables in name order, so the rendered
// container is byte-stable across runs, and skips names the container
// already sets: operator overrides survive and webhook reinvocation does
// not stack duplicates.
func mergeEnv(container *v1.Container, env map[string]string) {
	existing := make(map[string]struct{}, len(container.Env))
	for _, e := range container.Env {
		existing[e.Name] = struct{}{}
	}
	for _, name := range slices.Sorted(maps.Keys(env)) {
		if _, ok := existing[name]; ok {
			continue
		}
		container.Env = append(container.Env, v1.EnvVar{Name: name, Value: env[name]})
	}
}

// mergeMounts adds the profile's mounts the container does not have.
func mergeMounts(container *v1.Container, mounts []v1.VolumeMount) {
	for _, mount := range mounts {
		ensureVolumeMount(container, mount)
	}
}

// mergeSecurityContext adds what the profile demands and keeps the
// operator's choices: existing capabilities and Privileged are
// authoritative, only nil entries are filled. A profile without a
// security context (attachment-only) leaves the container untouched.
func mergeSecurityContext(container *v1.Container, profile *v1.SecurityContext) {
	if profile == nil {
		return
	}
	if container.SecurityContext == nil {
		container.SecurityContext = profile.DeepCopy()
		return
	}
	if container.SecurityContext.Capabilities == nil {
		container.SecurityContext.Capabilities = profile.Capabilities.DeepCopy()
	} else {
		for _, cap := range profile.Capabilities.Add {
			if !hasCapability(container.SecurityContext.Capabilities.Add, cap) {
				container.SecurityContext.Capabilities.Add = append(container.SecurityContext.Capabilities.Add, cap)
			}
		}
	}
	if container.SecurityContext.Privileged == nil {
		container.SecurityContext.Privileged = profile.Privileged
	}
}

// injectVolumes adds the profile's volumes the pod does not have.
func injectVolumes(pod *v1.Pod, volumes []v1.Volume) {
	for _, volume := range volumes {
		ensureVolume(pod, volume)
	}
}
