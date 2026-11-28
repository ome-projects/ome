package kvcachepool

import (
	"fmt"
	"strconv"

	"github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/kvcache"
)

const defaultServerImage = "lmcache/standalone:v0.5.1-cu129@sha256:c64d5704427e0c21e8b12d460f01a6cf33971ee2fdee2fbced268e4aa10d7d12"
const poolNameAnnotation = "ome.io/kvcachepool-name"

func childMetadata(pool *ome.KVCachePool, name string) metav1.ObjectMeta {
	// A foreground deletion may collect owner-referenced resources before the
	// pool finalizer has drained consumers. Only the finalizer deletes these.
	return metav1.ObjectMeta{
		Name:        name,
		Namespace:   pool.Namespace,
		Labels:      map[string]string{kvcache.ServerPoolUIDLabel: string(pool.UID)},
		Annotations: map[string]string{poolNameAnnotation: pool.Name},
	}
}

func serverDaemonSet(pool *ome.KVCachePool) (*appsv1.DaemonSet, error) {
	spec := &pool.Spec
	if spec.Provider.Name != ome.KVCacheProviderLMCache || spec.DeploymentMode != ome.KVCachePoolNodeLocal || len(spec.Workloads) != 1 || spec.Workloads[0].Name != "server" || len(spec.Workloads[0].NodeSelector) == 0 {
		return nil, fmt.Errorf("LMCache NodeLocal requires one server workload with a nonempty nodeSelector")
	}
	budget := spec.Provider.Config.L1CapacityPerNode
	if budget.Sign() <= 0 || (spec.Cache != nil && (spec.Cache.Capacity != nil || (spec.Cache.EvictionPolicy != "" && spec.Cache.EvictionPolicy != "LRU"))) {
		return nil, fmt.Errorf("LMCache NodeLocal requires a positive per-node L1 budget and supports LRU eviction")
	}
	workload := spec.Workloads[0]
	image := defaultServerImage
	resources := corev1.ResourceRequirements{}
	if runner := workload.Runner; runner != nil {
		if runner.Image != "" {
			if _, err := reference.ParseNormalizedNamed(runner.Image); err != nil {
				return nil, fmt.Errorf("invalid server runner.image %q: %w", runner.Image, err)
			}
			image = runner.Image
		}
		if runner.Resources != nil {
			resources = *runner.Resources.DeepCopy()
		}
	}
	// Reserve process overhead in addition to the configured host-memory cache.
	memory := budget.DeepCopy()
	memory.Add(resource.MustParse("5Gi"))
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	if _, ok := resources.Requests[corev1.ResourceCPU]; !ok {
		cpu := resource.MustParse("4")
		if limit, ok := resources.Limits[corev1.ResourceCPU]; ok && limit.Cmp(cpu) < 0 {
			cpu = limit
		}
		resources.Requests[corev1.ResourceCPU] = cpu
	}
	if _, ok := resources.Requests[corev1.ResourceMemory]; !ok {
		resources.Requests[corev1.ResourceMemory] = memory
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}
	if _, ok := resources.Limits[corev1.ResourceMemory]; !ok {
		resources.Limits[corev1.ResourceMemory] = resources.Requests[corev1.ResourceMemory]
	}
	if resources.Requests.Memory().Cmp(budget) <= 0 || resources.Limits.Memory().Cmp(*resources.Requests.Memory()) < 0 {
		return nil, fmt.Errorf("server memory request must exceed l1CapacityPerNode and fit within the memory limit")
	}
	for _, quantities := range []corev1.ResourceList{resources.Requests, resources.Limits} {
		if gpu := quantities["nvidia.com/gpu"]; gpu.Sign() != 0 {
			return nil, fmt.Errorf("the cache server shares the engine's GPUs and cannot request nvidia.com/gpu")
		}
	}
	rc := workload.RuntimeClassName
	if rc == "" {
		rc = "nvidia"
	}
	meta := childMetadata(pool, kvcache.ServerName(pool.UID))
	health := corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthcheck", Port: intstr.FromString("http")}}
	container := corev1.Container{
		Name:    "lmcache-server",
		Image:   image,
		Command: []string{"/opt/venv/bin/lmcache", "server"},
		Args: []string{
			"--host", "0.0.0.0",
			"--port", strconv.Itoa(kvcache.TransferPort),
			"--http-port", "8080",
			"--l1-size-gb", strconv.FormatFloat(budget.AsApproximateFloat64()/(1<<30), 'f', -1, 64),
			"--eviction-policy", "LRU",
		},
		Env: []corev1.EnvVar{
			{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
			{Name: "NVIDIA_DRIVER_CAPABILITIES", Value: "all"},
		},
		Resources: resources,
		Ports: []corev1.ContainerPort{
			{Name: "transfer", ContainerPort: kvcache.TransferPort},
			{Name: "http", ContainerPort: 8080},
		},
		StartupProbe:   &corev1.Probe{ProbeHandler: health, PeriodSeconds: 5, FailureThreshold: 60},
		ReadinessProbe: &corev1.Probe{ProbeHandler: health, PeriodSeconds: 5},
	}
	return &appsv1.DaemonSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"},
		ObjectMeta: meta,
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: meta.Labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: meta.Labels},
				Spec: corev1.PodSpec{
					NodeSelector:                 workload.NodeSelector,
					Tolerations:                  workload.Tolerations,
					HostIPC:                      true,
					RuntimeClassName:             &rc,
					AutomountServiceAccountToken: ptr.To(false),
					Containers:                   []corev1.Container{container},
				},
			},
		},
	}, nil
}

func serverService(pool *ome.KVCachePool) *corev1.Service {
	meta := childMetadata(pool, kvcache.ServerName(pool.UID))
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta,
		Spec: corev1.ServiceSpec{
			Type:                  corev1.ServiceTypeClusterIP,
			Selector:              meta.Labels,
			InternalTrafficPolicy: ptr.To(corev1.ServiceInternalTrafficPolicyLocal),
			Ports: []corev1.ServicePort{{
				Name: "transfer", Protocol: corev1.ProtocolTCP,
				Port: kvcache.TransferPort, TargetPort: intstr.FromString("transfer"),
			}},
		},
	}
}

func connectionMap(pool *ome.KVCachePool) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: childMetadata(pool, kvcache.ServerName(pool.UID)+"-connection"),
		Data:       map[string]string{kvcache.ConnectionKey: kvcache.ConnectionJSON(pool.UID, pool.Namespace)},
	}
}
