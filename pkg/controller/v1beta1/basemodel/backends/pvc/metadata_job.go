package pvc

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

const (
	// metadataJobScopeNamespaced / metadataJobScopeCluster are the values
	// written under constants.PVCMetadataScopeLabel. They mirror the
	// agent-side values in internal/ome-agent/model-metadata so the same
	// label can be used to filter both the Job and its ConfigMap.
	metadataJobScopeNamespaced = "namespaced"
	metadataJobScopeCluster    = "cluster"

	// metadataJobModelMountPath is where the metadata extractor expects the
	// model directory to be mounted.
	metadataJobModelMountPath = "/model"
	// metadataJobModelVolumeName is the in-pod name of the PVC volume.
	metadataJobModelVolumeName = "model"

	// metadataJobNameSuffix is appended to the model name to produce the
	// Job name. The total length is bounded by combining with a short hash.
	metadataJobNameSuffix = "-metadata-"

	// metadataJobAgentConfigPath is the path to the bundled ome-agent
	// config file inside the standard ome-agent image
	// (dockerfiles/ome-agent.Dockerfile). The agent's configProvider
	// (cmd/ome-agent/config.go) errors with "no config file provided"
	// if --config is empty, even though `model-metadata` reads all its
	// runtime params from --model-path / --basemodel-name / etc. flags.
	// Passing the bundled path satisfies the gratuitous existence check
	// without requiring a per-Job ConfigMap mount.
	metadataJobAgentConfigPath = "/ome-agent.yaml"
)

// MetadataJobConfig is the runtime configuration the BaseModel reconciler
// needs to spawn a metadata-extraction Job.
type MetadataJobConfig struct {
	// Image is the ome-agent container image (must include the
	// `model-metadata` subcommand — currently shipped in the same binary).
	Image string
	// ServiceAccount the Job pod runs as. Must have permission to
	// get/list/create/update/patch ConfigMaps in the OME namespace; the
	// agent writes a per-model status ConfigMap there. The same SA is
	// shared across BaseModel and ClusterBaseModel Jobs (the latter may
	// run in any namespace), so a cluster-scoped binding is required.
	ServiceAccount string
	// CPURequest / MemoryRequest / CPULimit / MemoryLimit follow the
	// standard Kubernetes resource shape; empty strings fall back to
	// Kubernetes defaults.
	CPURequest    string
	MemoryRequest string
	CPULimit      string
	MemoryLimit   string
	// BackoffLimit caps Job retries. Default is 2 if zero.
	BackoffLimit int32
	// TTLSecondsAfterFinished controls cleanup of completed Jobs. Default
	// 3600 if zero.
	TTLSecondsAfterFinished int32

	// NodeSelector / Tolerations / Affinity / PriorityClassName let
	// operators target the metadata Job onto specific nodes when the
	// PVC's underlying storage is restricted (e.g., a CSI driver only
	// available on a subset of nodes, or models that live on tainted
	// GPU nodes). Pass-through into the pod spec. All optional; nil
	// values mean "K8s default".
	NodeSelector      map[string]string
	Tolerations       []corev1.Toleration
	Affinity          *corev1.Affinity
	PriorityClassName string
}

// metadataJobShortHashLen is the number of hex chars used for the
// deterministic name suffix on both the metadata Job and the per-model
// status ConfigMap. Eight hex chars give ~10^9 keyspace per (model,uri)
// pair which is more than enough for non-cryptographic uniqueness.
const metadataJobShortHashLen = 8

// metadataJobName returns a deterministic, ≤63-char Job name derived from
// the model name and PVC URI. URI hashing means a spec edit that changes
// the URI yields a fresh Job rather than reusing a stale one.
func metadataJobName(modelName, storageURI string) string {
	sum := sha256.Sum256([]byte(storageURI))
	hash := hex.EncodeToString(sum[:])[:metadataJobShortHashLen]

	name := modelName + metadataJobNameSuffix + hash
	const maxNameLen = 63
	if len(name) > maxNameLen {
		// Trim the model-name portion; keep the suffix + hash intact for
		// determinism.
		excess := len(name) - maxNameLen
		trimmed := modelName[:len(modelName)-excess]
		// Guard against a fully-trimmed model name producing a leading
		// dash (DNS-1123 requires alphanumeric start). Fall back to a
		// hash-only name with a stable prefix.
		if trimmed == "" {
			return "metadata" + metadataJobNameSuffix + hash
		}
		name = trimmed + metadataJobNameSuffix + hash
	}
	return name
}

// metadataJobLabels are the labels a model's metadata-extraction Job and its
// pod carry: the model name and its scope. The cleanup paths find the Jobs by
// them, because the Jobs live in the PVC's namespace, which only the URI names.
func metadataJobLabels(obj client.Object, isClusterScoped bool) map[string]string {
	scope := metadataJobScopeNamespaced
	if isClusterScoped {
		scope = metadataJobScopeCluster
	}
	return map[string]string{
		constants.PVCMetadataModelNameLabel: obj.GetName(),
		constants.PVCMetadataScopeLabel:     scope,
	}
}

// buildMetadataJob constructs (without creating) the Job that runs
// `ome-agent model-metadata` against a PVC-mounted model directory.
//
// The Job is placed in pvcNamespace (which equals the BaseModel's namespace
// for namespaced BaseModel, and the URI-specified namespace for
// ClusterBaseModel). An OwnerReference to obj is attached so the Job is
// garbage-collected with its owner; this works for namespaced BaseModel
// (same-ns Job) and for cluster-scoped ClusterBaseModel (cluster-scoped
// owners may own children in any namespace).
func buildMetadataJob(obj client.Object, isClusterScoped bool, components *storage.PVCStorageComponents, pvcNamespace string, cfg MetadataJobConfig, scheme *runtime.Scheme) (*batchv1.Job, error) {
	if obj == nil || components == nil {
		return nil, fmt.Errorf("buildMetadataJob: nil object or components")
	}
	if cfg.Image == "" {
		return nil, fmt.Errorf("buildMetadataJob: ome-agent image must be configured")
	}

	storageURI, err := storageURIOf(obj)
	if err != nil {
		return nil, err
	}

	jobName := metadataJobName(obj.GetName(), storageURI)
	jobLabels := metadataJobLabels(obj, isClusterScoped)

	args := []string{
		"model-metadata",
		"--config", metadataJobAgentConfigPath,
		"--model-path", metadataJobModelMountPath,
		"--basemodel-name", obj.GetName(),
	}
	if isClusterScoped {
		args = append(args, "--cluster-scoped")
	} else {
		args = append(args, "--basemodel-namespace", obj.GetNamespace())
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pvcNamespace,
			Labels:    jobLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To(cmp.Or(cfg.BackoffLimit, int32(2))),
			TTLSecondsAfterFinished: ptr.To(cmp.Or(cfg.TTLSecondsAfterFinished, int32(3600))),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: jobLabels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: cfg.ServiceAccount,
					RestartPolicy:      corev1.RestartPolicyNever,
					SecurityContext:    metadataJobPodSecurityContext(),
					NodeSelector:       cfg.NodeSelector,
					Tolerations:        cfg.Tolerations,
					Affinity:           cfg.Affinity,
					PriorityClassName:  cfg.PriorityClassName,
					Volumes: []corev1.Volume{
						{
							Name: metadataJobModelVolumeName,
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: components.PVCName,
									ReadOnly:  true,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "model-metadata",
							Image:           cfg.Image,
							Args:            args,
							SecurityContext: metadataJobContainerSecurityContext(),
							// constants.OMENamespace reads the controller
							// pod's POD_NAMESPACE at process start (default
							// "ome"). The agent uses the same constant
							// to compute the per-PVC status ConfigMap's
							// namespace, so without this env on the Job
							// pod an OME install in (e.g.) "ome-prod"
							// would have the controller writing RBAC in
							// "ome-prod" while the agent tried to write
							// the ConfigMap in "ome" — perpetual
							// "configmap not found" on reconcile.
							Env: []corev1.EnvVar{
								{Name: "POD_NAMESPACE", Value: constants.OMENamespace},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      metadataJobModelVolumeName,
									MountPath: metadataJobModelMountPath,
									SubPath:   components.SubPath,
									ReadOnly:  true,
								},
							},
							Resources: buildResourceRequirements(cfg),
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(obj, job, scheme); err != nil {
		return nil, fmt.Errorf("set owner reference: %w", err)
	}
	return job, nil
}

// buildResourceRequirements assembles a ResourceRequirements from the
// optional CPU/memory strings on cfg. Empty strings are skipped.
func buildResourceRequirements(cfg MetadataJobConfig) corev1.ResourceRequirements {
	requests := corev1.ResourceList{}
	limits := corev1.ResourceList{}
	if cfg.CPURequest != "" {
		requests[corev1.ResourceCPU] = resource.MustParse(cfg.CPURequest)
	}
	if cfg.MemoryRequest != "" {
		requests[corev1.ResourceMemory] = resource.MustParse(cfg.MemoryRequest)
	}
	if cfg.CPULimit != "" {
		limits[corev1.ResourceCPU] = resource.MustParse(cfg.CPULimit)
	}
	if cfg.MemoryLimit != "" {
		limits[corev1.ResourceMemory] = resource.MustParse(cfg.MemoryLimit)
	}
	rr := corev1.ResourceRequirements{}
	if len(requests) > 0 {
		rr.Requests = requests
	}
	if len(limits) > 0 {
		rr.Limits = limits
	}
	return rr
}

func storageURIOf(obj client.Object) (string, error) {
	spec, _, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return "", err
	}
	if spec.Storage == nil || spec.Storage.StorageUri == nil {
		return "", fmt.Errorf("storage URI is not set on %s/%s", obj.GetNamespace(), obj.GetName())
	}
	return *spec.Storage.StorageUri, nil
}

// metadataJobNonRootUID is the UID/GID the agent runs as. 65532 is the
// nonroot user shipped in distroless / Chainguard base images, which the
// upstream ome-agent Dockerfile uses (or is compatible with).
const metadataJobNonRootUID int64 = 65532

// metadataJobPodSecurityContext returns the pod-level security context
// required to satisfy PodSecurity admission's `restricted` profile.
func metadataJobPodSecurityContext() *corev1.PodSecurityContext {
	uid := metadataJobNonRootUID
	return &corev1.PodSecurityContext{
		RunAsNonRoot:   ptr.To(true),
		RunAsUser:      &uid,
		RunAsGroup:     &uid,
		FSGroup:        &uid,
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// metadataJobContainerSecurityContext returns the container-level security
// context required to satisfy PodSecurity admission's `restricted` profile.
func metadataJobContainerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
		// SeccompProfile already set at the pod level; restating it on the
		// container guards against the pod default being weakened by an
		// admission mutator.
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}
