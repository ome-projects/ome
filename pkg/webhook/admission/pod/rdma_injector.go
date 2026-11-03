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
	// DefaultRDMAProfile is the default RDMA profile to use if none is specified
	DefaultRDMAProfile = "oci-roce"
	// DevInfVolumeName is the name of the /dev/infiniband volume
	DevInfVolumeName = "devinf"
	// DshmVolumeName is the name of the /dev/shm volume
	DshmVolumeName = "dshm"
	// DefaultContainerName is the default container name to inject into if not specified
	DefaultContainerName = "ome-container"
	// MultusNetworksAnnotation is the pod annotation Multus reads to attach
	// extra NetworkAttachmentDefinitions. Value is a JSON array of
	// {name, namespace} objects.
	MultusNetworksAnnotation = "k8s.v1.cni.cncf.io/networks"
)

// RDMAProfiles is a map of profile names to RDMA configurations
var RDMAProfiles = map[string]RDMAProfile{
	"oci-roce": {
		// UCX_TLS and UCX_NET_DEVICES are deliberately absent. No NCCL
		// path reads them, but UCX-based KV transfer (NIXL) does, and
		// pinning them to tcp/eth0 quietly moved it off RDMA (#837).
		// Runtimes that want a UCX restriction set it on the container.
		EnvVars: map[string]string{
			"NCCL_NET_PLUGIN":            "none",
			"NCCL_DEBUG":                 "INFO",
			"NCCL_CROSS_NIC":             "2",
			"NCCL_SOCKET_NTHREADS":       "16",
			"NCCL_CUMEM_ENABLE":          "0",
			"NCCL_IB_SPLIT_DATA_ON_QPS":  "0",
			"NCCL_IB_QPS_PER_CONNECTION": "16",
			"NCCL_IB_GID_INDEX":          "3",
			"NCCL_IB_HCA":                "=mlx5_0,mlx5_1,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_7,mlx5_8,mlx5_9,mlx5_10,mlx5_12,mlx5_13,mlx5_14,mlx5_15,mlx5_16,mlx5_17",
			"NCCL_IB_TC":                 "41",
			"NCCL_IB_SL":                 "0",
			"NCCL_IB_TIMEOUT":            "22",
			"HCOLL_ENABLE_MCAST_ALL":     "0",
			"coll_hcoll_enable":          "0",
			"RX_QUEUE_LEN":               "8192",
			"IB_RX_QUEUE_LEN":            "8192",
			"NCCL_SOCKET_IFNAME":         "eth0",
			"NCCL_IGNORE_CPU_AFFINITY":   "1",
			"GLOO_SOCKET_IFNAME":         "eth0",
		},
		VolumeMounts: []v1.VolumeMount{
			{
				Name:      DshmVolumeName,
				MountPath: "/dev/shm",
			},
			{
				Name:      DevInfVolumeName,
				MountPath: "/dev/infiniband",
			},
		},
		Volumes: []v1.Volume{
			{
				Name: DshmVolumeName,
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{
						Medium: v1.StorageMediumMemory,
					},
				},
			},
			{
				Name: DevInfVolumeName,
				VolumeSource: v1.VolumeSource{
					HostPath: &v1.HostPathVolumeSource{
						Path: "/dev/infiniband",
					},
				},
			},
		},
		// IPC_LOCK is what RDMA verbs need for pinned memory. Opening the
		// HCA device nodes is not granted here: the /dev/infiniband mount
		// above only makes them visible, and the device cgroup still
		// denies them. Runtimes get that access by requesting their
		// cluster's RDMA device-plugin resource, or by setting privileged
		// explicitly — values already on the container win over the
		// profile. The profile itself is deliberately not privileged:
		// that bypasses the device cgroup and hands the pod every GPU on
		// the node, not the ones the NVIDIA device plugin allocated (#833).
		SecurityContext: &v1.SecurityContext{
			Capabilities: &v1.Capabilities{
				Add: []v1.Capability{"IPC_LOCK"},
			},
		},
	},
	// cks-gb-sglang: SGLang PD-serving env baseline.
	// Volumes/mounts and securityContext match oci-roce so runtimes can
	// swap profiles without touching the rest of the pod.
	"cks-gb-sglang": {
		EnvVars: map[string]string{
			"MC_TE_METRIC":        "true",
			"NCCL_MNNVL_ENABLE":   "1",
			"NCCL_CUMEM_ENABLE":   "1",
			"NCCL_IB_ADDR_FAMILY": "AF_INET6",
			"NCCL_SOCKET_IFNAME":  "eth0",
			"NCCL_SOCKET_FAMILY":  "AF_INET6",
		},
		VolumeMounts: []v1.VolumeMount{
			{Name: DshmVolumeName, MountPath: "/dev/shm"},
			{Name: DevInfVolumeName, MountPath: "/dev/infiniband"},
		},
		Volumes: []v1.Volume{
			{
				Name: DshmVolumeName,
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{Medium: v1.StorageMediumMemory},
				},
			},
			{
				Name: DevInfVolumeName,
				VolumeSource: v1.VolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/dev/infiniband"},
				},
			},
		},
		// Not privileged, for the same reason as oci-roce above.
		SecurityContext: &v1.SecurityContext{
			Capabilities: &v1.Capabilities{
				Add: []v1.Capability{"IPC_LOCK"},
			},
		},
	},
	// cks-gb-rdma: Multus network attachments only (no env/volumes/security)
	// for the GB-class CKS topology (GB200 + GB300 share 4 HCAs × 4 ports =
	// 16 ibs*-macvlan attachments in cw-multus). CoreWeave provisions the
	// NADs but does NOT auto-attach them, so each pod needing NIXL/UCX over
	// RoCEv2 must request them explicitly. Workload-specific knobs
	// (NCCL/UCX env, /dev/shm sizing, securityContext) stay in the runtime
	// YAML — they vary per model.
	"cks-gb-rdma": {
		Networks: cksGbRdmaNetworks,
	},
}

// cksGbRdmaNetworks is the canonical 16-attachment list for CKS GB200/GB300
// (4 HCAs × 4 ports), matching the node's net-device layout.
var cksGbRdmaNetworks = func() []NetworkAttachment {
	out := make([]NetworkAttachment, 0, 16)
	for hca := 0; hca < 4; hca++ {
		for port := 0; port < 4; port++ {
			out = append(out, NetworkAttachment{
				Name:      fmt.Sprintf("ibs%dp%d-macvlan", hca, port),
				Namespace: "cw-multus",
			})
		}
	}
	return out
}()

// NetworkAttachment is a Multus network reference. Serialized into
// k8s.v1.cni.cncf.io/networks per the multus-cni JSON schema.
type NetworkAttachment struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// RDMAProfile represents configuration parameters for RDMA and NCCL
type RDMAProfile struct {
	EnvVars         map[string]string
	VolumeMounts    []v1.VolumeMount
	Volumes         []v1.Volume
	SecurityContext *v1.SecurityContext
	// Networks are merged into the pod's k8s.v1.cni.cncf.io/networks
	// annotation; dedup is by (name, namespace).
	Networks []NetworkAttachment
}

// injectRDMA applies an RDMA profile (env / volumes / Multus
// attachments) when the ome.io/rdma-auto-inject=true annotation is set.
// The profile name defaults to DefaultRDMAProfile, overridable via
// ome.io/rdma-profile.
func injectRDMA(pod *v1.Pod) error {
	if autoInject, ok := pod.ObjectMeta.Annotations[constants.RDMAAutoInjectAnnotationKey]; !ok || autoInject != "true" {
		return nil
	}
	profileName := DefaultRDMAProfile
	if profile, ok := pod.ObjectMeta.Annotations[constants.RDMAProfileAnnotationKey]; ok && profile != "" {
		profileName = profile
	}
	profile, ok := RDMAProfiles[profileName]
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
