package constants

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/network"
)

// OME Constants
var (
	OMEName         = "ome"
	OMEAPIGroupName = "ome.io"
	OMENamespace    = getEnvOrDefault("POD_NAMESPACE", "ome")

	// OMENativeExecutorCapabilitySchemaAnnotationKey identifies the migration
	// request schema consumed by the active InferenceReplica executor.
	OMENativeExecutorCapabilitySchemaAnnotationKey = OMEAPIGroupName + "/migration-request-schema"
)

// OMENativeExecutorCapabilityLeaseName is the namespaced heartbeat published
// by the active InferenceReplica executor manager.
const OMENativeExecutorCapabilityLeaseName = "ome-inferencereplica-executor"

// InferenceService Constants
var (
	InferenceServiceName          = "inferenceservice"
	InferenceServiceAPIName       = "inferenceservices"
	InferenceServicePodLabelKey   = OMEAPIGroupName + "/" + InferenceServiceName
	InferenceServiceConfigMapName = "inferenceservice-config"
	BaseModelFinalizer            = "basemodels.ome.io/finalizer"
	ClusterBaseModelFinalizer     = "clusterbasemodels.ome.io/finalizer"
	AcceleratorClassFinalizer     = "acceleratorclasses.ome.io/finalizer"
	// AcceleratorClassAnnotationKey pins an InferenceService to a named
	// AcceleratorClass; runtime auto-selection rejects runtimes that do not
	// support the class.
	AcceleratorClassAnnotationKey = OMEAPIGroupName + "/accelerator-class"
)

// OME Agent Constants
var (
	AgentName                         = "ome-agent"
	AgentAppName                      = "OME_AGENT"
	AgentModelNameEnvVarKey           = AgentAppName + "_" + "MODEL_NAME"
	AgentModelStoreDirectoryEnvVarKey = AgentAppName + "_" + "MODEL_STORE_DIRECTORY"
	AgentModelFrameworkEnvVarKey      = AgentAppName + "_" + "MODEL_FRAMEWORK"
	AgentBaseModelTypeEnvVarKey       = AgentAppName + "_" + "MODEL_TYPE"

	// General Configuration
	AgentLocalPathEnvVarKey       = AgentAppName + "_" + "LOCAL_PATH"
	AgentNumOfGPUEnvVarKey        = AgentAppName + "_" + "NUM_OF_GPU"
	AgentModelBucketNameEnvVarKey = AgentAppName + "_" + "MODEL_BUCKET_NAME"
	AgentModelNamespaceEnvVarKey  = AgentAppName + "_" + "MODEL_NAMESPACE"
	AgentModelObjectName          = AgentAppName + "_" + "MODEL_OBJECT_NAME"

	// OCI Authentication
	AgentCompartmentIDEnvVarKey = AgentAppName + "_" + "COMPARTMENT_ID"
	AgentAuthTypeEnvVarKey      = AgentAppName + "_" + "AUTH_TYPE"
	AgentRegionEnvVarKey        = AgentAppName + "_" + "REGION"

	// Serving Sidecar Configuration
	AgentFineTunedWeightInfoFilePath      = AgentAppName + "_" + "FINE_TUNED_WEIGHT_INFO_FILE_PATH"
	AgentUnzippedFineTunedWeightDirectory = AgentAppName + "_" + "UNZIPPED_FINE_TUNED_WEIGHT_DIRECTORY"
	AgentZippedFineTunedWeightDirectory   = AgentAppName + "_" + "ZIPPED_FINE_TUNED_WEIGHT_DIRECTORY"
)

// InferenceService MultiModel Constants

var (
	ModelConfigFileName = "models.json"
)

// Model agent Constants
const (
	AgentConfigMapKeyName = "agent"
	TensorRTLLM           = "tensorrtllm"
)

// InferenceService Annotations
var (
	DeploymentMode                          = OMEAPIGroupName + "/deploymentMode"
	EnableRoutingTagAnnotationKey           = OMEAPIGroupName + "/enable-tag-routing"
	AutoscalerClass                         = OMEAPIGroupName + "/autoscalerClass"
	AutoscalerPropagatedMetadataKeys        = OMEAPIGroupName + "/autoscaler-propagated-metadata-keys"
	AutoscalerMetrics                       = OMEAPIGroupName + "/metrics"
	TargetUtilizationPercentage             = OMEAPIGroupName + "/targetUtilizationPercentage"
	DeprecationWarning                      = OMEAPIGroupName + "/deprecation-warning"
	EnableMetricAggregation                 = OMEAPIGroupName + "/enable-metric-aggregation"
	SetPrometheusAnnotation                 = OMEAPIGroupName + "/enable-prometheus-scraping"
	DedicatedAICluster                      = OMEAPIGroupName + "/dedicated-ai-cluster"
	VolcanoQueue                            = OMEAPIGroupName + "/volcano-queue"
	FineTunedAdapterInjectionKey            = OMEAPIGroupName + "/inject-fine-tuned-adapter"
	ServingSidecarInjectionKey              = OMEAPIGroupName + "/inject-serving-sidecar"
	FineTunedWeightFTStrategyKey            = OMEAPIGroupName + "/fine-tuned-weight-ft-strategy"
	BaseModelName                           = OMEAPIGroupName + "/base-model-name"
	BaseModelVendorAnnotationKey            = OMEAPIGroupName + "/base-model-vendor"
	ServingRuntimeKeyName                   = OMEAPIGroupName + "/serving-runtime"
	BaseModelFormat                         = OMEAPIGroupName + "/base-model-format"
	BaseModelFormatVersion                  = OMEAPIGroupName + "/base-model-format-version"
	FTServingWithMergedWeightsAnnotationKey = OMEAPIGroupName + "/fine-tuned-serving-with-merged-weights"
	ServiceType                             = OMEAPIGroupName + "/service-type"
	LoadBalancerIP                          = OMEAPIGroupName + "/load-balancer-ip"
	EntrypointComponent                     = OMEAPIGroupName + "/entrypoint-component"
	ContainerPrometheusPortKey              = "prometheus.ome.io/port"
	ContainerPrometheusPathKey              = "prometheus.ome.io/path"
	// ExtraPodMetricsEndpointsAnnotationKey declares ADDITIONAL PodMonitor
	// scrape endpoints appended to the default /metrics endpoint. Value is a
	// comma-separated list of "portName:path" (port name, not number), e.g.
	// "http:/engine_metrics". Additive and backward-compatible; malformed
	// entries are skipped.
	ExtraPodMetricsEndpointsAnnotationKey = "prometheus.ome.io/extra-endpoints"
	PrometheusPortAnnotationKey           = "prometheus.io/port"
	PrometheusPathAnnotationKey           = "prometheus.io/path"
	PrometheusScrapeAnnotationKey         = "prometheus.io/scrape"
	RDMAAutoInjectAnnotationKey           = "rdma.ome.io/auto-inject"
	RDMAProfileAnnotationKey              = "rdma.ome.io/profile"
	RDMAContainerNameAnnotationKey        = "rdma.ome.io/container-name"

	// Runtime profile injection — opt-in profiles that factor out
	// repetitive ServingRuntime YAML (probes, /dev/shm, prometheus
	// scrape annotations). Each is gated on its own profile annotation;
	// the container-name annotation is shared across all of them.
	RuntimeContainerNameAnnotationKey        = "runtime.ome.io/container-name"
	RuntimeShmProfileAnnotationKey           = "runtime.ome.io/shm-profile"
	RuntimeProbeProfileAnnotationKey         = "runtime.ome.io/probe-profile"
	RuntimeProbePortAnnotationKey            = "runtime.ome.io/probe-port"
	RuntimeObservabilityProfileAnnotationKey = "runtime.ome.io/observability-profile"
	RuntimeObservabilityPortAnnotationKey    = "runtime.ome.io/observability-port"

	// ServingRuntime inheritance + profile marker.
	RuntimeProfileAnnotationKey     = OMEAPIGroupName + "/runtime-profile"
	RuntimeInheritFromAnnotationKey = OMEAPIGroupName + "/inherit-from"
	RuntimeInheritMaxDepth          = 5
	InheritanceReadyConditionType   = "InheritanceReady"
	// Engine-preset annotation. Value selects an
	// OME-shipped preset (e.g. "sglang-pd") whose engine defaults a
	// mutating webhook injects into the runtime spec at admission.
	RuntimeEngineAnnotationKey = OMEAPIGroupName + "/engine"

	// Bumping this annotation on an ISVC tells the controller to
	// advance the pinned ControllerRevision to a fresh snapshot of
	// the current runtime spec.
	RuntimeSyncAnnotationKey = OMEAPIGroupName + "/runtime-sync"

	// RuntimeDrifted is True when a pinned ISVC's live runtime hash
	// differs from the pinned revision's hash. Reasons:
	//   RevisionMismatch — live spec drifted from pin
	//   RevisionMissing  — pinned revision GC'd or deleted
	//   PinAdvanced      — transient, set during ack
	RuntimeDriftedConditionType = "RuntimeDrifted"

	// Labels on ControllerRevisions the OME controller creates for
	// pinning. The writer uses them to find-or-create by content
	// hash; the GC loop uses them to list per-source-runtime.
	RuntimeRevisionOfLabelKey          = OMEAPIGroupName + "/runtime-of"
	RuntimeRevisionOfKindLabelKey      = OMEAPIGroupName + "/runtime-of-kind"
	RuntimeRevisionOfNamespaceLabelKey = OMEAPIGroupName + "/runtime-of-namespace"
	RuntimeRevisionHashLabelKey        = OMEAPIGroupName + "/revision-hash"

	// Gates the immutability webhook so StatefulSet/DaemonSet-owned
	// ControllerRevisions (no annotation) pass through unchanged.
	RuntimeRevisionCreatedByKey      = OMEAPIGroupName + "/created-by"
	RuntimeRevisionCreatedByOMEValue = "ome-controller"

	// Annotation the GC controller sets on a revision the first time
	// it observes it as unreferenced + over the retention count.
	// Holds an RFC3339 timestamp; the GC controller deletes the
	// revision when (now - value) exceeds the configured grace period.
	// Cleared if the revision becomes referenced again before then.
	RuntimeRevisionGCEligibleSinceKey = OMEAPIGroupName + "/gc-eligible-since"

	DefaultPrometheusPath                    = "/metrics"
	QueueProxyAggregatePrometheusMetricsPort = 9088
	DefaultPodPrometheusPort                 = "9091"
	ModelCategoryAnnotation                  = "models.ome.io/category"

	// Ingress Configuration Overrides
	IngressDomainTemplate          = OMEAPIGroupName + "/ingress-domain-template"
	IngressDomain                  = OMEAPIGroupName + "/ingress-domain"
	IngressAdditionalDomains       = OMEAPIGroupName + "/ingress-additional-domains"
	IngressURLScheme               = OMEAPIGroupName + "/ingress-url-scheme"
	IngressPathTemplate            = OMEAPIGroupName + "/ingress-path-template"
	IngressDisableIstioVirtualHost = OMEAPIGroupName + "/ingress-disable-istio-virtualhost"
	IngressDisableCreation         = OMEAPIGroupName + "/ingress-disable-creation"
	IngressConsistentHashHeaders   = OMEAPIGroupName + "/ingress-consistent-hash-headers"
	// Gateway/host selection overrides (per-ISVC; mirror the cluster ingress config
	// so a single ISVC can target a different gateway or host scheme).
	IngressGatewayOverride    = OMEAPIGroupName + "/ingress-gateway"
	IngressPerISVCSubdomain   = OMEAPIGroupName + "/ingress-per-isvc-subdomain"
	IngressSharedHostPrefix   = OMEAPIGroupName + "/ingress-shared-host-prefix"
	IngressAdditionalGateways = OMEAPIGroupName + "/ingress-additional-gateways"

	// InferenceReplica is a controller-only CRD: the validating webhook
	// rejects direct user writes unless this annotation is set to "true".
	// The ISVC controller (and only the ISVC controller) stamps the
	// annotation on every Create / Update of an InferenceReplica it owns.
	// Convention enforcement, not a security boundary — RBAC restricting
	// write on inferencereplicas to the OME ServiceAccount is the actual
	// guard.
	InferenceReplicaControllerWriteAnnotationKey = OMEAPIGroupName + "/controller-write"
	InferenceReplicaControllerWriteAnnotationVal = "true"

	// ReleaseHeldRevisionAnnotationKey names a Held RetryBlock by full ControllerRevision
	// name or bare hash. The controller consumes it after handling the release request.
	ReleaseHeldRevisionAnnotationKey = OMEAPIGroupName + "/release-held-revision"

	// RevisionExcludedAnnotationKeysAnnotationKey lists inherited ISVC annotation keys
	// omitted from the pod-template revision hash; component annotations remain hash inputs.
	RevisionExcludedAnnotationKeysAnnotationKey = OMEAPIGroupName + "/revision-excluded-annotation-keys"

	// InferenceReplicaParentGenerationAnnotationKey records the parent
	// ISVC's metadata.generation the projector most recently applied to
	// this InferenceReplica. Coordination gates compare it against the
	// live ISVC generation to tell "this Component's projection hasn't
	// caught up with the operator's latest spec bump" apart from "this
	// Component genuinely has nothing to roll" — the IR's own
	// status.observedGeneration cannot answer that, because it tracks
	// the IR's generation, which only moves when the projected spec
	// itself changes.
	InferenceReplicaParentGenerationAnnotationKey = OMEAPIGroupName + "/parent-generation"
)

// Label Constants
var (
	VolcanoQueueName                      = "volcano.sh/queue-name"
	InferenceServiceBaseModelNameLabelKey = "base-model-name"
	InferenceServiceBaseModelSizeLabelKey = "base-model-size"
	BaseModelTypeLabelKey                 = "base-model-type"
	BaseModelVendorLabelKey               = "base-model-vendor"
	FTServingLabelKey                     = "fine-tuned-serving"
	FTServingWithMergedWeightsLabelKey    = "fine-tuned-serving-with-merged-weights"
	ServingRuntimeLabelKey                = "serving-runtime"
	FineTunedWeightFTStrategyLabelKey     = "fine-tuned-weight-ft-strategy"
)

// PrioriryClass
var (
	DedicatedAiClusterPreemptionWorkloadPriorityClass = "kueue-scheduling-high-priority"
)

// InferenceService Internal Annotations
var (
	InferenceServiceInternalAnnotationsPrefix           = "internal." + OMEAPIGroupName
	StorageInitializerSourceUriInternalAnnotationKey    = InferenceServiceInternalAnnotationsPrefix + "/storage-initializer-sourceuri"
	InferenceServiceInPlaceImageTransitionAnnotationKey = InferenceServiceInternalAnnotationsPrefix + "/in-place-image-transition"
)

// ome networking constants
const (
	NetworkVisibility      = "networking.ome.io/visibility"
	ClusterLocalVisibility = "cluster-local"
	ClusterLocalDomain     = "svc.cluster.local"
	IsvcNameHeader         = "OMe-Isvc-Name"
	IsvcNamespaceHeader    = "OME-Isvc-Namespace"
)

// Controller Constants
var (
	DefaultMinReplicas = 1

	IstioSidecarInjectionLabel = "sidecar.istio.io/inject"
)

type AutoscalerClassType string
type AutoscalerMetricsType string
type AutoScalerKPAMetricsType string

// Autoscaler Default Class
var (
	DefaultAutoscalerClass = AutoscalerClassHPA
)

// Autoscaler Class
var (
	AutoscalerClassHPA      AutoscalerClassType = "hpa"
	AutoscalerClassKEDA     AutoscalerClassType = "keda"
	AutoscalerClassExternal AutoscalerClassType = "external"
)

// Autoscaler Metrics
var (
	AutoScalerMetricsCPU AutoscalerMetricsType = "cpu"
)

// Autoscaler Memory metrics
var (
	AutoScalerMetricsMemory AutoscalerMetricsType = "memory"
)

// Autoscaler Class Allowed List
var AutoscalerAllowedClassList = []AutoscalerClassType{
	AutoscalerClassHPA,
	AutoscalerClassKEDA,
	AutoscalerClassExternal,
}

// Autoscaler Metrics Allowed List
var AutoscalerAllowedMetricsList = []AutoscalerMetricsType{
	AutoScalerMetricsCPU,
	AutoScalerMetricsMemory,
}

// Autoscaler Default Metrics Value
var (
	DefaultCPUUtilization int32 = 80
)

// Webhook Constants
var (
	PodMutatorWebhookName              = OMEName + "-pod-mutator-webhook"
	ServingRuntimeValidatorWebhookName = OMEName + "-servingRuntime-validator-webhook"
)

// GPU/CPU resource constants
const (
	NvidiaGPUResourceType = "nvidia.com/gpu"
	GoogleTPUResourceType = "google.com/tpu"
)

// InferenceService Environment Variables
const (
	ContainerPrometheusMetricsPortEnvVarKey           = "CONTAINER_PROMETHEUS_METRICS_PORT"
	ContainerPrometheusMetricsPathEnvVarKey           = "CONTAINER_PROMETHEUS_METRICS_PATH"
	QueueProxyAggregatePrometheusMetricsPortEnvVarKey = "AGGREGATE_PROMETHEUS_METRICS_PORT"

	TFewWeightPathEnvVarKey = "TFEW_PATH"

	ModelPathEnvVarKey                   = "MODEL_PATH"
	ServedModelNameEnvVarKey             = "SERVED_MODEL_NAME"
	ModelCacheProviderEnvVarKey          = "MODEL_CACHE_PROVIDER"
	ModelCacheEndpointEnvVarKey          = "MODEL_CACHE_ENDPOINT"
	ModelCacheOptionsEnvVarKey           = "MODEL_CACHE_OPTIONS"
	ClusterCacheHeadlessServiceEnvVarKey = "CLUSTER_CACHE_HEADLESS_SERVICE"

	ParallelismSizeEnvVarKey = "PARALLELISM_SIZE"
)

// ModelConfig Constants
const (
	ModelConfigKey = "models.json"
)

const (
	SentenceTransformersConfigFileName = "config_sentence_transformers.json"
)

type InferenceServiceComponent string

type InferenceServiceVerb string

type InferenceServiceProtocol string

// VisibilityLabel is the cluster-local visibility marker. Kept for
// backward compatibility with manifests that still carry the Knative
// idiom even though OME no longer serves Knative.
const (
	VisibilityLabel = "networking.knative.dev/visibility"
)

// InferenceService Component enums
const (
	Predictor InferenceServiceComponent = "predictor"
	Router    InferenceServiceComponent = "router"
	Engine    InferenceServiceComponent = "engine"
	Decoder   InferenceServiceComponent = "decoder"
)

// InferenceService protocol enums
const (
	OpenAIProtocol          InferenceServiceProtocol = "openAI"
	OpenInferenceProtocolV1 InferenceServiceProtocol = "openInference-v1"
	OpenInferenceProtocolV2 InferenceServiceProtocol = "openInference-v2"
)

// InferenceService Endpoint Ports
const (
	InferenceServiceDefaultHttpPort     = "8080"
	InferenceServiceDefaultAgentPortStr = "9081"
	InferenceServiceDefaultAgentPort    = 9081
	CommonDefaultHttpPort               = 80
	CommonISVCPort                      = 8080
	AggregateMetricsPortName            = "aggr-metric"
)

// Labels to put on kservice
const (
	OMEComponentLabel = "component"
	OMEEndpointLabel  = "endpoint"
)

// Labels for TrainedModel
const (
	InferenceServiceLabel = "ome.io/inferenceservice"
)

// InferenceService default/canary constants
const (
	InferenceServiceDefault = "default"
)

// DAC/InferenceService/TrainingJob container names
const (
	MainContainerName               = "ome-container"
	TrainingMainContainerName       = "trainer"
	StorageInitializerContainerName = "storage-initializer"
	FineTunedAdapterContainerName   = "fine-tuned-adapter"
	ServingSidecarContainerName     = "serving-sidecar"
)

// Model Agents Constants
const (
	AuthtypeOKEWorkloadIdentity = "OkeWorkloadIdentity"
)

// Serving Container Block Lists
const (
	BlocklistConfigMapVolumeName = "configmap-blocklist-volume"
	InputBlocklistSubPath        = "input.txt"
	OutputBlocklistSubPath       = "output.txt"
	InputBlocklistMountPath      = "/usr/bin/input.txt"
	OutputBlocklistMountPath     = "/usr/bin/output.txt"
)

// Cohere volume mount paths
const (
	ModelEmptyDirVolumeName                   = "model-empty-dir"
	ModelDefaultSourcePath                    = "/mnt/model"
	ModelDefaultMountPathPrefix               = "/opt/ml"
	ModelDefaultMountPath                     = "/opt/ml/model"
	FineTunedWeightDownloadMountPath          = "/mnt/finetuned/download"
	CohereTFewFineTunedWeightVolumeMountPath  = "/opt/ml/tfew"
	CohereTFewFineTunedWeightDefaultPath      = "/opt/ml/tfew/fastertransformer/1"
	BaseModelVolumeMountSubPath               = "base"
	FineTunedWeightDownloadVolumeMountSubPath = "download"
	FineTunedWeightVolumeMountSubPath         = "finetuned"
	TensorRTModelVolumeMountSubPath           = "tensorrt_llm"
)

// Constants used for inference container arguments
const (
	LLamaVllmServedModelNameArgName         = "--served-model-name"
	LLamaVllmFTServingServedModelNamePrefix = "/data"
)

// DefaultModelLocalMountPath is where models will be mounted by the storage-initializer
const DefaultModelLocalMountPath = "/mnt/models"

var (
	ServiceAnnotationDisallowedList = []string{
		StorageInitializerSourceUriInternalAnnotationKey,
		"kubectl.kubernetes.io/last-applied-configuration",
		// Rollout operator verbs are ISVC-level signals consumed by the
		// controller off the InferenceService; they must not propagate to
		// managed pods/Services. (The revision layer also strips them from
		// the pod-template hash so toggling one never mints a revision.)
		RolloutPromoteAnnotation,
		RolloutRollbackAnnotation,
	}

	RevisionTemplateLabelDisallowedList = []string{
		VisibilityLabel,
	}

	// PodOnlyAnnotationPrefixes contains annotation prefixes (or exact keys) that should only
	// be applied to Pods, not to Services. These are typically used for pod-level configurations
	// like monitoring, networking interfaces, container injection, and other pod-specific settings.
	//
	// Entries can be either:
	// - Prefix patterns ending with "/" (e.g., "k8s.grafana.com/") - matches all annotations with this prefix
	// - Exact annotation keys (e.g., FineTunedAdapterInjectionKey) - matches only that specific annotation
	//
	// Both styles work correctly because IsPrefixSupported uses strings.HasPrefix for matching.
	PodOnlyAnnotationPrefixes = []string{
		"k8s.grafana.com/",           // Grafana scraping annotations (k8s.grafana.com/scrape, k8s.grafana.com/port)
		"loki.grafana.com/",          // Loki log collection annotations (loki.grafana.com/scrape, loki.grafana.com/log-format)
		"prometheus.io/",             // Prometheus scraping annotations (prometheus.io/scrape, prometheus.io/port, prometheus.io/path)
		"networking.gke.io/",         // GKE multi-NIC and RDMA network annotations (networking.gke.io/interfaces, etc.)
		"rdma.ome.io/",               // OME RDMA injection annotations (RDMAAutoInjectAnnotationKey, RDMAProfileAnnotationKey, etc.)
		"runtime.ome.io/",            // OME runtime profile annotations (shm/probe/observability profiles + container-name override)
		ModelInitInjectionKey,        // ome.io/inject-model-init - triggers model init container injection via webhook
		FineTunedAdapterInjectionKey, // ome.io/inject-fine-tuned-adapter - triggers fine-tuned adapter injection via webhook
		ServingSidecarInjectionKey,   // ome.io/inject-serving-sidecar - triggers serving sidecar injection via webhook
	}
)

// CheckResultType raw k8s deployment, resource exist check result
type CheckResultType int

const (
	CheckResultCreate  CheckResultType = 0
	CheckResultUpdate  CheckResultType = 1
	CheckResultExisted CheckResultType = 2
	CheckResultUnknown CheckResultType = 3
	CheckResultDelete  CheckResultType = 4
	CheckResultSkipped CheckResultType = 5
)

type DeploymentModeType string

const (
	RawDeployment     DeploymentModeType = "RawDeployment"
	PDDisaggregated   DeploymentModeType = "PDDisaggregated"
	MultiNode         DeploymentModeType = "MultiNode"
	VirtualDeployment DeploymentModeType = "VirtualDeployment"
	OMENative         DeploymentModeType = "OMENative"
)

// IsValid checks if the deployment mode is valid
func (d DeploymentModeType) IsValid() bool {
	switch d {
	case RawDeployment, MultiNode, VirtualDeployment, OMENative:
		return true
	default:
		return false
	}
}

// app label
const (
	RawDeploymentAppLabel = "app"
)

// container state reason
const (
	StateReasonRunning          = "Running"
	StateReasonCompleted        = "Completed"
	StateReasonError            = "Error"
	StateReasonCrashLoopBackOff = "CrashLoopBackOff"
)

// CRD Kinds
const (
	VolcanoQueueKind     = "Queue"
	KEDAScaledObjectKind = "ScaledObject"
	VolcanoJobKind       = "Job"
	LWSKind              = "LeaderWorkerSet"
	GatewayKind          = "Gateway"
	ServiceKind          = "Service"
	PodMonitorKind       = "PodMonitor"
	// PodGroupKind is the scheduler-plugins coscheduling PodGroup
	// (`scheduling.x-k8s.io/v1alpha1`). Optional — OMENative degrades
	// gracefully when the CRD is absent (sets the
	// `GangSchedulingUnavailable=True` Component Condition).
	PodGroupKind = "PodGroup"
)

// Volcano Job Labels
const (
	VolcanoJobLabelName = "volcano.sh/job-name"
)

// Kueue related Labels
const (
	KueueQueueLabelKey                 = "kueue.x-k8s.io/queue-name"
	KueueWorkloadPriorityClassLabelKey = "kueue.x-k8s.io/priority-class"
	KueueEnabledLabelKey               = "kueue-enabled"
)

// Model Agent & Model Controller
var (
	NodeInstanceShapeLabel           = "node.kubernetes.io/instance-type"
	DeprecatedNodeInstanceShapeLabel = "beta.kubernetes.io/instance-type"
	ModelsLabelPrefix                = "models.ome/"
	TargetInstanceShapes             = "models.ome.io/target-instance-shapes"
	ModelStatusConfigMapLabel        = "models.ome/basemodel-status"
	ReserveModelArtifact             = "models.ome/reserve-model-artifact"

	// PVCStorageConfigMapLabel marks a model status ConfigMap as belonging
	// to a PVC-backed BaseModel (rather than to a node). The BaseModel
	// controller skips its per-node existence check on these.
	PVCStorageConfigMapLabel = "models.ome/pvc-status"
	// PVCMetadataModelNameLabel records the BaseModel/ClusterBaseModel name
	// the PVC ConfigMap belongs to (for human filtering).
	PVCMetadataModelNameLabel = "models.ome/model-name"
	// PVCMetadataScopeLabel is "namespaced" or "cluster".
	PVCMetadataScopeLabel = "models.ome/model-scope"
	// PVCMetadataLastErrorAnnotation captures the last extraction error
	// message on the PVC ConfigMap, for operator debugging.
	PVCMetadataLastErrorAnnotation = "models.ome/pvc-metadata-last-error"

	ModelLabelDomain          = "models.ome.io"
	ClusterBaseModelLabelType = "clusterbasemodel"
	BaseModelLabelType        = "basemodel"
)

type TrainingStrategy string

const (
	TFewTrainingStrategy TrainingStrategy = "tfew"
	LoraTrainingStrategy TrainingStrategy = "lora"
)

type ServingStrategy string

// Default training job constants
const (
	TrainingJobName                   = "trainingjob"
	MergedModelWeightZippedFileSuffix = "-merged-weight"
)

type TrainingSidecarRuntime string

type TrainingRuntimeType string

// Training sidecar env variable key names and config key names

var (
	TrainingJobPodLabelKey = OMEAPIGroupName + "/" + TrainingJobName
)

var (
	StrategyConfigKey = "strategy"
)

// FineTunedWeight related constants
const (
	FineTunedWeightMergedWeightsConfigKey = "merged_weights"
)

type ModelVendor string

const (
	Meta   ModelVendor = "meta"
	Cohere ModelVendor = "cohere"
	OpenAI ModelVendor = "openai"
)

// BaseModelType enum
type BaseModelType string

const (
	ServingBaseModel BaseModelType = "Serving"
)

const (
	BaseModel                 string = "BaseModel"
	ClusterBaseModel          string = "ClusterBaseModel"
	LowerCaseBaseModel        string = "basemodel"
	LowerCaseClusterBaseModel string = "clusterbasemodel"
)

func (c CheckResultType) String() string {
	switch c {
	case CheckResultCreate:
		return "Create"
	case CheckResultUpdate:
		return "Update"
	case CheckResultExisted:
		return "Existed"
	case CheckResultUnknown:
		return "Unknown"
	case CheckResultDelete:
		return "Delete"
	case CheckResultSkipped:
		return "Skipped"
	default:
		return "Invalid"
	}
}

func GetModelsLabelWithUid(uid types.UID) string {
	return ModelsLabelPrefix + string(uid)
}

// GetRawServiceLabel returns the shared label value for Raw workload resources.
func GetRawServiceLabel(service string) string {
	return TruncateNameWithMaxLength(service, k8svalidation.LabelValueMaxLength)
}

func (e InferenceServiceComponent) String() string {
	return string(e)
}

func (v InferenceServiceVerb) String() string {
	return string(v)
}

func getEnvOrDefault(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func ModelConfigName(isvcName string) string {
	var maxLen = 20
	if len(isvcName) > maxLen {
		isvcName = isvcName[len(isvcName)-maxLen:]
	}
	return fmt.Sprintf("modelconfig-%s", isvcName)
}

func LWSName(componentName string) string {
	const (
		lwsNamePrefix = "lws-"
		// Leave room for downstream prefixes/suffixes when this name is embedded in Pod label values.
		maxLWSNameLength = 35
	)

	return TruncateNameWithPrefix(componentName, lwsNamePrefix, maxLWSNameLength)
}

func InferenceServiceHostName(name string, namespace string, domain string) string {
	var builder strings.Builder
	// Pre-allocate capacity: name + "." + namespace + "." + domain
	builder.Grow(len(name) + len(namespace) + len(domain) + 2)
	builder.WriteString(name)
	builder.WriteByte('.')
	builder.WriteString(namespace)
	builder.WriteByte('.')
	builder.WriteString(domain)
	return builder.String()
}

func DefaultPredictorServiceName(name string) string {
	var builder strings.Builder
	predictorStr := string(Predictor)
	// Pre-allocate capacity: name + "-" + predictorStr + "-" + InferenceServiceDefault
	builder.Grow(len(name) + len(predictorStr) + len(InferenceServiceDefault) + 2)
	builder.WriteString(name)
	builder.WriteByte('-')
	builder.WriteString(predictorStr)
	builder.WriteByte('-')
	builder.WriteString(InferenceServiceDefault)
	return builder.String()
}

func DefaultRouterServiceName(name string) string {
	return name + "-" + string(Router) + "-" + InferenceServiceDefault
}

func PredictorServiceName(name string) string {
	return name
}

func RouterServiceName(name string) string {
	return name + "-router"
}

func DecoderServiceName(name string) string {
	return name + "-decoder"
}

func EngineServiceName(name string) string {
	return name + "-engine"
}

func DecoderPrefix() string {
	return "^/v1/.*$"
}

func PathBasedExplainPrefix() string {
	return "(/v1/.*)$"
}

// FallbackPrefix returns the regex pattern to match any path
func FallbackPrefix() string {
	return "^/.*$"
}

// Should only match 1..65535, but for simplicity it matches 0-99999.
const portMatch = `(?::\d{1,5})?`

// HostRegExp returns an ECMAScript regular expression to match either host or host:<any port>
// for clusterLocalHost, we will also match the prefixes.
func HostRegExp(host string) string {
	localDomainSuffix := ".svc." + network.GetClusterDomainName()
	if !strings.HasSuffix(host, localDomainSuffix) {
		return exact(regexp.QuoteMeta(host) + portMatch)
	}
	prefix := regexp.QuoteMeta(strings.TrimSuffix(host, localDomainSuffix))
	clusterSuffix := regexp.QuoteMeta("." + network.GetClusterDomainName())
	svcSuffix := regexp.QuoteMeta(".svc")
	return exact(prefix + optional(svcSuffix+optional(clusterSuffix)) + portMatch)
}

func exact(regexp string) string {
	return "^" + regexp + "$"
}

func optional(regexp string) string {
	return "(" + regexp + ")?"
}

// Kubernetes naming constraints
const (
	// Maximum length for label names (after domain/)
	MaxLabelNameLength = 49 // 63 - 13 - 1 (ModelLabelDomain length)
	// Maximum length for ConfigMap keys
	MaxConfigMapKeyLength = 253
	// Length of hash prefix when truncating
	HashPrefixLength = 8
)

// truncateWithHashTweaks truncates a string to maxLength, taking suffix and adding hash prefix for uniqueness
// If the original string fits within maxLength, it's returned as-is
// Otherwise, it returns: {hash_prefix}-{suffix}
// If the return starts with a numeric character, prepend a leading character
func truncateWithHashTweaks(original string, maxLength int) string {
	if len(original) <= maxLength {
		return original
	}

	// Generate hash of the original string
	hasher := sha256.New()
	hasher.Write([]byte(original))
	hashBytes := hasher.Sum(nil)
	hashPrefix := hex.EncodeToString(hashBytes)[:HashPrefixLength]

	if hashPrefix[0] >= '0' && hashPrefix[0] <= '9' {
		// Replace the first char with a letter but keep the rest
		hashPrefix = "a" + hashPrefix[1:]
	}

	// Calculate available space for suffix (minus hash prefix and separator)
	suffixLength := maxLength - HashPrefixLength - 1
	if suffixLength <= 0 {
		// If maxLength is too small, just return hash
		return hashPrefix[:maxLength]
	}

	// Take suffix from original string
	suffix := original[len(original)-suffixLength:]

	return fmt.Sprintf("%s-%s", hashPrefix, suffix)
}

// truncateWithHash truncates a string to maxLength, taking suffix and adding hash prefix for uniqueness
// If the original string fits within maxLength, it's returned as-is
// Otherwise, it returns: {hash_prefix}-{suffix}
func truncateWithHash(original string, maxLength int) string {
	if len(original) <= maxLength {
		return original
	}

	// Generate hash of the original string
	hasher := sha256.New()
	hasher.Write([]byte(original))
	hashBytes := hasher.Sum(nil)
	hashPrefix := hex.EncodeToString(hashBytes)[:HashPrefixLength]

	// Calculate available space for suffix (minus hash prefix and separator)
	suffixLength := maxLength - HashPrefixLength - 1
	if suffixLength <= 0 {
		// If maxLength is too small, just return hash
		return hashPrefix[:maxLength]
	}

	// Take suffix from original string
	suffix := original[len(original)-suffixLength:]

	return fmt.Sprintf("%s-%s", hashPrefix, suffix)
}

// GetClusterBaseModelLabel returns the deterministic label key for ClusterBaseModel
// Format: models.ome.io/clusterbasemodel.{model_name}
// Handles long model names by truncating with hash for uniqueness
func GetClusterBaseModelLabel(modelName string) string {
	// Available space: MaxLabelNameLength - "clusterbasemodel." = 49 - 17 = 32
	maxModelNameLength := MaxLabelNameLength - len(ClusterBaseModelLabelType) - 1
	if len(modelName) <= maxModelNameLength {
		// No truncation needed
		return fmt.Sprintf("%s/%s.%s", ModelLabelDomain, ClusterBaseModelLabelType, modelName)
	}
	truncatedModelName := truncateWithHash(modelName, maxModelNameLength)
	return fmt.Sprintf("%s/%s.%s", ModelLabelDomain, ClusterBaseModelLabelType, truncatedModelName)
}

// GetBaseModelLabel returns the deterministic label key for BaseModel
// Format: models.ome.io/{namespace}.basemodel.{model_name}
// Handles long names by truncating with hash for uniqueness
func GetBaseModelLabel(namespace, modelName string) string {
	// Available space: MaxLabelNameLength - "basemodel." = 49 - 10 = 39
	// Need to split between namespace and modelName
	baseLength := len(BaseModelLabelType) + 1             // "basemodel."
	availableSpace := MaxLabelNameLength - baseLength - 1 // -1 for separator between namespace and basemodel

	// Check if both namespace and model name fit without truncation
	totalNeeded := len(namespace) + len(modelName)
	if totalNeeded <= availableSpace {
		// No truncation needed
		return fmt.Sprintf("%s/%s.%s.%s", ModelLabelDomain, namespace, BaseModelLabelType, modelName)
	}

	// Truncation needed - split available space, giving priority to model name
	minLength := 8
	var namespaceMaxLength, modelNameMaxLength int

	if availableSpace < minLength*2 {
		// If total space is too small, truncate both equally
		namespaceMaxLength = availableSpace / 2
		modelNameMaxLength = availableSpace - namespaceMaxLength
	} else {
		// Give model name more space, but ensure namespace gets at least minLength
		if len(namespace) <= minLength {
			// Namespace is short enough, give remaining space to model name
			namespaceMaxLength = len(namespace)
			modelNameMaxLength = availableSpace - namespaceMaxLength
		} else {
			// Namespace needs truncation, allocate minimum to namespace
			namespaceMaxLength = minLength
			modelNameMaxLength = availableSpace - namespaceMaxLength
		}
	}

	truncatedNamespace := truncateWithHash(namespace, namespaceMaxLength)
	truncatedModelName := truncateWithHash(modelName, modelNameMaxLength)

	return fmt.Sprintf("%s/%s.%s.%s", ModelLabelDomain, truncatedNamespace, BaseModelLabelType, truncatedModelName)
}

// GetModelConfigMapKey returns the deterministic ConfigMap key for models
// For ClusterBaseModel: clusterbasemodel.{model_name}
// For BaseModel: {namespace}.basemodel.{model_name}
// Handles long names by truncating with hash for uniqueness
func GetModelConfigMapKey(namespace, modelName string, isClusterBaseModel bool) string {
	if isClusterBaseModel {
		// Available space: MaxConfigMapKeyLength - "clusterbasemodel." = 253 - 17 = 236
		maxModelNameLength := MaxConfigMapKeyLength - len(ClusterBaseModelLabelType) - 1
		if len(modelName) <= maxModelNameLength {
			// No truncation needed
			return fmt.Sprintf("%s.%s", ClusterBaseModelLabelType, modelName)
		}
		truncatedModelName := truncateWithHash(modelName, maxModelNameLength)
		return fmt.Sprintf("%s.%s", ClusterBaseModelLabelType, truncatedModelName)
	}

	// For BaseModel: {namespace}.basemodel.{model_name}
	// Available space: MaxConfigMapKeyLength - "basemodel." = 253 - 10 = 243
	baseLength := len(BaseModelLabelType) + 1                // "basemodel."
	availableSpace := MaxConfigMapKeyLength - baseLength - 1 // -1 for separator between namespace and basemodel

	// Check if both namespace and model name fit without truncation
	totalNeeded := len(namespace) + len(modelName)
	if totalNeeded <= availableSpace {
		// No truncation needed
		return fmt.Sprintf("%s.%s.%s", namespace, BaseModelLabelType, modelName)
	}

	// Truncation needed - split available space between namespace and model name
	minLength := 8
	var namespaceMaxLength, modelNameMaxLength int

	if availableSpace < minLength*2 {
		namespaceMaxLength = availableSpace / 2
		modelNameMaxLength = availableSpace - namespaceMaxLength
	} else {
		// Give model name priority
		if len(namespace) <= minLength {
			// Namespace is short enough, give remaining space to model name
			namespaceMaxLength = len(namespace)
			modelNameMaxLength = availableSpace - namespaceMaxLength
		} else {
			// Namespace needs truncation, allocate minimum to namespace
			namespaceMaxLength = minLength
			modelNameMaxLength = availableSpace - namespaceMaxLength
		}
	}

	truncatedNamespace := truncateWithHash(namespace, namespaceMaxLength)
	truncatedModelName := truncateWithHash(modelName, modelNameMaxLength)

	return fmt.Sprintf("%s.%s.%s", truncatedNamespace, BaseModelLabelType, truncatedModelName)
}

// pvcMetadataConfigMapPrefix and PVCMetadataNameHashLen together define the
// shape of the per-PVC-model status ConfigMap name, shared between the
// BaseModel controller (reads) and the metadata-extraction agent (writes).
const (
	pvcMetadataConfigMapPrefix = "pvc-metadata-"
	PVCMetadataNameHashLen     = 8
)

// GetPVCMetadataConfigMapName returns the deterministic, ≤63-char name of
// the per-PVC-model status ConfigMap. Unlike per-node ConfigMaps (which
// are named after the node and host one entry per model), the PVC variant
// is one ConfigMap per PVC-backed model — there is no node concept for
// PVC storage.
//
// Both the controller and the agent compute this name independently so
// the agent can write and the controller can read by exact name (no List
// scan needed).
func GetPVCMetadataConfigMapName(modelName, modelNamespace string, isClusterScoped bool) string {
	keySource := modelName
	if !isClusterScoped {
		keySource = modelNamespace + "/" + modelName
	}
	sum := sha256.Sum256([]byte(keySource))
	return pvcMetadataConfigMapPrefix + hex.EncodeToString(sum[:])[:PVCMetadataNameHashLen]
}

// TruncateNameWithMaxLength return a valid DNS name
func TruncateNameWithMaxLength(name string, maxLength int) string {
	return truncateWithHashTweaks(name, maxLength)
}

// ParseModelInfoFromConfigMapKey attempts to parse model information from a ConfigMap key
// Returns namespace, modelName, isClusterBaseModel, and whether parsing was successful
func ParseModelInfoFromConfigMapKey(configMapKey string) (namespace, modelName string, isClusterBaseModel bool, success bool) {
	// Try to parse as ClusterBaseModel
	if strings.HasPrefix(configMapKey, ClusterBaseModelLabelType+".") {
		modelName = strings.TrimPrefix(configMapKey, ClusterBaseModelLabelType+".")
		return "", modelName, true, true
	}

	// Try to parse as BaseModel: {namespace}.basemodel.{modelName}
	if strings.Contains(configMapKey, "."+BaseModelLabelType+".") {
		parts := strings.SplitN(configMapKey, "."+BaseModelLabelType+".", 2)
		if len(parts) == 2 {
			return parts[0], parts[1], false, true
		}
	}

	return "", "", false, false
}

// Env keys the agent's model-decryption and artifact-reuse paths consume.
var (
	AgentKeyNameEnvVarKey                    = AgentAppName + "_" + "KEY_NAME"
	AgentSecretNameEnvVarKey                 = AgentAppName + "_" + "SECRET_NAME"
	AgentVaultIDEnvVarKey                    = AgentAppName + "_" + "VAULT_ID"
	AgentDisableModelDecryptionEnvVarKey     = AgentAppName + "_" + "DISABLE_MODEL_DECRYPTION"
	AgentTargetArtifactReuseAllowedEnvVarKey = AgentAppName + "_" + "TARGET_ARTIFACT_REUSE_ALLOWED"
	AgentTensorRTLLMVersionsEnvVarKey        = AgentAppName + "_" + "TENSORRTLLM_VERSION"
)

// Model decryption and model-init injection annotation keys.
var (
	BaseModelDecryptionKeyName    = OMEAPIGroupName + "/base-model-decryption-key-name"
	BaseModelDecryptionSecretName = OMEAPIGroupName + "/base-model-decryption-secret-name"
	DisableModelDecryption        = OMEAPIGroupName + "/disable-model-decryption"
	ModelInitInjectionKey         = OMEAPIGroupName + "/inject-model-init"
	ModelInitContainerName        = "model-init"
)

// OCI-artifact reuse markers: sentinel objects written next to a model's
// artifacts so concurrent downloaders can detect a complete or in-flight
// upload.
const (
	ArtifactCompleteMarkerFileName = ".ome-artifact-complete"
	ArtifactCompleteMarkerBody     = "complete\n"
	ArtifactUploadLockFileName     = ".ome-artifact-upload.lock"
	ArtifactUploadLockBody         = "uploading\n"
	HfArtifactConfigMapKeyPrefix   = "artifact.huggingface."
	// ModelArtifactsDirectory contains node-local artifacts shared by model paths.
	ModelArtifactsDirectory = "_artifacts"
)

// IsArtifactCompleteMarkerObjectName reports whether objectName is (or ends
// with) the artifact-complete sentinel.
func IsArtifactCompleteMarkerObjectName(objectName string) bool {
	return objectName == ArtifactCompleteMarkerFileName || strings.HasSuffix(objectName, "/"+ArtifactCompleteMarkerFileName)
}

// IsArtifactUploadLockObjectName reports whether objectName is (or ends with)
// the artifact upload-lock sentinel.
func IsArtifactUploadLockObjectName(objectName string) bool {
	return objectName == ArtifactUploadLockFileName || strings.HasSuffix(objectName, "/"+ArtifactUploadLockFileName)
}

// IsInternalArtifactObjectName reports whether objectName is one of the
// artifact bookkeeping sentinels rather than model content.
func IsInternalArtifactObjectName(objectName string) bool {
	return IsArtifactCompleteMarkerObjectName(objectName) || IsArtifactUploadLockObjectName(objectName)
}

// BenchmarkJob names.
const (
	BenchmarjJobName          = "benchmarkjob"
	BenchmarkJobConfigMapName = "benchmarkjob-config"
)

// BenchmarkJobValidatorWebhookName is the BenchmarkJob validating-webhook name.
var BenchmarkJobValidatorWebhookName = OMEName + "-benchmark-job-validator-webhook"

// Alfred caretaker annotations: per-workload operator knobs gating the
// Alfred optimization policies.
var (
	// AlfredAPIGroupName groups the per-workload caretaker annotations an
	// operator sets on an InferenceService to gate Alfred's policies.
	AlfredAPIGroupName = "alfred.ome.io"
	// AlfredMovableAnnotationKey opts a workload out of ("false") or into
	// ("true") every Alfred policy; wins over the cluster-wide default.
	AlfredMovableAnnotationKey = AlfredAPIGroupName + "/movable"
	// AlfredPriorityAnnotationKey is a float in [0, 1]; lower = more
	// protected when Alfred orders candidates.
	AlfredPriorityAnnotationKey = AlfredAPIGroupName + "/priority"
	// AlfredSpotPolicyAnnotationKey overrides the cluster spot policy for
	// this workload: avoid | migrate | ignore.
	AlfredSpotPolicyAnnotationKey = AlfredAPIGroupName + "/spot-policy"
	// AlfredCooldownMinutesAnnotationKey overrides the per-workload
	// migration cooldown for this workload.
	AlfredCooldownMinutesAnnotationKey = AlfredAPIGroupName + "/cooldown-minutes"
	// AlfredTenantGroupAnnotationKey opts same-group workloads across
	// namespaces into cross-tenant optimization.
	AlfredTenantGroupAnnotationKey = AlfredAPIGroupName + "/tenant-group"
	// AlfredOptOutReasonAnnotationKey is a free-text operator note
	// explaining an opt-out; surfaced in events, never parsed.
	AlfredOptOutReasonAnnotationKey = AlfredAPIGroupName + "/opt-out-reason"
	// MigrationRequestAnnotationPrefix prefixes the UUID-suffixed
	// migration-request annotations (`ome.io/migration-request-v1-<uuid>`)
	// written onto an InferenceService. The workload audit package carries
	// the same value for the controller-side consumer.
	MigrationRequestAnnotationPrefix = OMEAPIGroupName + "/migration-request-v1-"
)

// Annotation-driven KEDA scaling keys and defaults.
const (
	KedaScalingOperator         = "autoscaling.keda.sh/operator"
	KedaScalingThreshold        = "autoscaling.keda.sh/threshold"
	KedaPrometheusQuery         = "autoscaling.keda.sh/prometheus.query"
	KedaPrometheusServerAddress = "autoscaling.keda.sh/prometheus.serverAddress"
	KedaDefaultMinReplicas      = 1
)

// Istio and scheduling names.
const (
	IstioMeshGateway        = "mesh"
	IstioVirtualServiceKind = "VirtualService"
	VolcanoScheduler        = "volcano"

	DedicatedAiClusterPreemptionPriorityClass = "volcano-scheduling-high-priority"

	// RevisionLabel is the pod label used to look up pods for non-raw deployment modes.
	// The key retains its historical serving.knative.dev prefix.
	RevisionLabel = "serving.knative.dev/revision"
)

// TruncateNameWithPrefix truncates name to fit maxLength together with the
// given prefix, preserving the prefix whole when possible.
func TruncateNameWithPrefix(name, prefix string, maxLength int) string {
	if maxLength <= 0 {
		return ""
	}

	payloadMaxLength := maxLength - len(prefix)
	if payloadMaxLength <= 0 {
		return TruncateNameWithMaxLength(prefix, maxLength)
	}

	return prefix + TruncateNameWithMaxLength(name, payloadMaxLength)
}
