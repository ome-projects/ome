package constants

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"

	rayutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/network"
	"knative.dev/serving/pkg/apis/autoscaling"
)

// OME Constants
var (
	OMEName                          = "ome"
	OMEAPIGroupName                  = "ome.io"
	KnativeAutoscalingAPIGroupName   = "autoscaling.knative.dev"
	KnativeServingAPIGroupNamePrefix = "serving.knative"
	KnativeServingAPIGroupName       = KnativeServingAPIGroupNamePrefix + ".dev"
	OMENamespace                     = getEnvOrDefault("POD_NAMESPACE", "ome")
)

// Benchmark Constants
var (
	BenchmarjJobName          = "benchmarkjob"
	BenchmarkJobConfigMapName = "benchmarkjob-config"
)

// InferenceService Constants
var (
	InferenceServiceName          = "inferenceservice"
	InferenceServiceAPIName       = "inferenceservices"
	InferenceServicePodLabelKey   = OMEAPIGroupName + "/" + InferenceServiceName
	InferenceServiceConfigMapName = "inferenceservice-config"
	BaseModelFinalizer            = "basemodels.ome.io/finalizer"
	ClusterBaseModelFinalizer     = "clusterbasemodels.ome.io/finalizer"
	AcceleratorClassFinalizer     = "acceleratorclasses.ome.io/finalizer"
)

// OME Agent Constants
var (
	AgentName                         = "ome-agent"
	AgentAppName                      = "OME_AGENT"
	AgentModelNameEnvVarKey           = AgentAppName + "_" + "MODEL_NAME"
	AgentModelStoreDirectoryEnvVarKey = AgentAppName + "_" + "MODEL_STORE_DIRECTORY"
	AgentModelFrameworkEnvVarKey      = AgentAppName + "_" + "MODEL_FRAMEWORK"
	AgentTensorRTLLMVersionsEnvVarKey = AgentAppName + "_" + "TENSORRTLLM_VERSION"
	AgentBaseModelTypeEnvVarKey       = AgentAppName + "_" + "MODEL_TYPE"

	// General Configuration
	AgentLocalPathEnvVarKey              = AgentAppName + "_" + "LOCAL_PATH"
	AgentNumOfGPUEnvVarKey               = AgentAppName + "_" + "NUM_OF_GPU"
	AgentDisableModelDecryptionEnvVarKey = AgentAppName + "_" + "DISABLE_MODEL_DECRYPTION"
	AgentModelBucketNameEnvVarKey        = AgentAppName + "_" + "MODEL_BUCKET_NAME"
	AgentModelNamespaceEnvVarKey         = AgentAppName + "_" + "MODEL_NAMESPACE"
	AgentModelObjectName                 = AgentAppName + "_" + "MODEL_OBJECT_NAME"

	// OCI Vault and Security
	AgentCompartmentIDEnvVarKey = AgentAppName + "_" + "COMPARTMENT_ID"
	AgentAuthTypeEnvVarKey      = AgentAppName + "_" + "AUTH_TYPE"
	AgentRegionEnvVarKey        = AgentAppName + "_" + "REGION"
	AgentVaultIDEnvVarKey       = AgentAppName + "_" + "VAULT_ID"
	AgentKeyNameEnvVarKey       = AgentAppName + "_" + "KEY_NAME"
	AgentSecretNameEnvVarKey    = AgentAppName + "_" + "SECRET_NAME"

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
	DeploymentMode                           = OMEAPIGroupName + "/deploymentMode"
	EnableRoutingTagAnnotationKey            = OMEAPIGroupName + "/enable-tag-routing"
	AutoscalerClass                          = OMEAPIGroupName + "/autoscalerClass"
	AutoscalerMetrics                        = OMEAPIGroupName + "/metrics"
	TargetUtilizationPercentage              = OMEAPIGroupName + "/targetUtilizationPercentage"
	DeprecationWarning                       = OMEAPIGroupName + "/deprecation-warning"
	MinScaleAnnotationKey                    = KnativeAutoscalingAPIGroupName + "/min-scale"
	MaxScaleAnnotationKey                    = KnativeAutoscalingAPIGroupName + "/max-scale"
	RollOutDurationAnnotationKey             = KnativeServingAPIGroupName + "/rollout-duration"
	KnativeOpenshiftEnablePassthroughKey     = "serving.knative.openshift.io/enablePassthrough"
	EnableMetricAggregation                  = OMEAPIGroupName + "/enable-metric-aggregation"
	SetPrometheusAnnotation                  = OMEAPIGroupName + "/enable-prometheus-scraping"
	DedicatedAICluster                       = OMEAPIGroupName + "/dedicated-ai-cluster"
	VolcanoQueue                             = OMEAPIGroupName + "/volcano-queue"
	ModelInitInjectionKey                    = OMEAPIGroupName + "/inject-model-init"
	FineTunedAdapterInjectionKey             = OMEAPIGroupName + "/inject-fine-tuned-adapter"
	ServingSidecarInjectionKey               = OMEAPIGroupName + "/inject-serving-sidecar"
	FineTunedWeightFTStrategyKey             = OMEAPIGroupName + "/fine-tuned-weight-ft-strategy"
	BaseModelName                            = OMEAPIGroupName + "/base-model-name"
	BaseModelVendorAnnotationKey             = OMEAPIGroupName + "/base-model-vendor"
	ServingRuntimeKeyName                    = OMEAPIGroupName + "/serving-runtime"
	BaseModelFormat                          = OMEAPIGroupName + "/base-model-format"
	BaseModelFormatVersion                   = OMEAPIGroupName + "/base-model-format-version"
	FTServingWithMergedWeightsAnnotationKey  = OMEAPIGroupName + "/fine-tuned-serving-with-merged-weights"
	ServiceType                              = OMEAPIGroupName + "/service-type"
	LoadBalancerIP                           = OMEAPIGroupName + "/load-balancer-ip"
	EntrypointComponent                      = OMEAPIGroupName + "/entrypoint-component"
	ContainerPrometheusPortKey               = "prometheus.ome.io/port"
	ContainerPrometheusPathKey               = "prometheus.ome.io/path"
	PrometheusPortAnnotationKey              = "prometheus.io/port"
	PrometheusPathAnnotationKey              = "prometheus.io/path"
	PrometheusScrapeAnnotationKey            = "prometheus.io/scrape"
	RDMAAutoInjectAnnotationKey              = "rdma.ome.io/auto-inject"
	RDMAProfileAnnotationKey                 = "rdma.ome.io/profile"
	RDMAContainerNameAnnotationKey           = "rdma.ome.io/container-name"
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
)

// InferenceService Annotations for model encryption and decryption
var (
	BaseModelDecryptionKeyName    = OMEAPIGroupName + "/base-model-decryption-key-name"
	BaseModelDecryptionSecretName = OMEAPIGroupName + "/base-model-decryption-secret-name"
	DisableModelDecryption        = OMEAPIGroupName + "/disable-model-decryption"
)

// Label Constants
var (
	RayScheduler                          = "ray.io/scheduler-name"
	RayPrioriyClass                       = "ray.io/priority-class-name"
	RayClusterUnavailableSince            = "raycluster/unavailable-since"
	VolcanoQueueName                      = "volcano.sh/queue-name"
	VolcanoScheduler                      = "volcano"
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
	DedicatedAiClusterPreemptionPriorityClass = "volcano-scheduling-high-priority"

	DedicatedAiClusterPreemptionWorkloadPriorityClass = "kueue-scheduling-high-priority"
)

// InferenceService Internal Annotations
var (
	InferenceServiceInternalAnnotationsPrefix        = "internal." + OMEAPIGroupName
	StorageInitializerSourceUriInternalAnnotationKey = InferenceServiceInternalAnnotationsPrefix + "/storage-initializer-sourceuri"
)

// ome networking constants
const (
	NetworkVisibility      = "networking.ome.io/visibility"
	ClusterLocalVisibility = "cluster-local"
	ClusterLocalDomain     = "svc.cluster.local"
	IsvcNameHeader         = "OMe-Isvc-Name"
	IsvcNamespaceHeader    = "OME-Isvc-Namespace"
)

// StorageSpec Constants
var ()

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

// Keda Autoscaler Configs
var (
	KedaScalingThreshold        = "autoscaling.keda.sh/threshold"
	KedaScalingOperator         = "autoscaling.keda.sh/operator"
	KedaPrometheusServerAddress = "autoscaling.keda.sh/prometheus.serverAddress"
	KedaPrometheusQuery         = "autoscaling.keda.sh/prometheus.query"
	KedaDefaultMinReplicas      = 1
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
	BenchmarkJobValidatorWebhookName   = OMEName + "-benchmark-job-validator-webhook"
)

// GPU/CPU resource constants
const (
	NvidiaGPUResourceType = "nvidia.com/gpu"
)

// InferenceService Environment Variables
const (
	ContainerPrometheusMetricsPortEnvVarKey           = "CONTAINER_PROMETHEUS_METRICS_PORT"
	ContainerPrometheusMetricsPathEnvVarKey           = "CONTAINER_PROMETHEUS_METRICS_PATH"
	QueueProxyAggregatePrometheusMetricsPortEnvVarKey = "AGGREGATE_PROMETHEUS_METRICS_PORT"

	TFewWeightPathEnvVarKey = "TFEW_PATH"

	ModelPathEnvVarKey       = "MODEL_PATH"
	ServedModelNameEnvVarKey = "SERVED_MODEL_NAME"

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

// Knative constants
const (
	KnativeLocalGateway   = "knative-serving/knative-local-gateway"
	KnativeIngressGateway = "knative-serving/knative-ingress-gateway"
	VisibilityLabel       = "networking.knative.dev/visibility"
)

var (
	LocalGatewayHost = "knative-local-gateway.istio-system.svc." + network.GetClusterDomainName()
	IstioMeshGateway = "mesh"
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
	MultiNodeProberContainerName    = "multinode-prober"
	StorageInitializerContainerName = "storage-initializer"
	ModelInitContainerName          = "model-init"
	FineTunedAdapterContainerName   = "fine-tuned-adapter"
	ServingSidecarContainerName     = "serving-sidecar"
	MultiNodeProberContainerPort    = 8080
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
		autoscaling.MinScaleAnnotationKey,
		autoscaling.MaxScaleAnnotationKey,
		StorageInitializerSourceUriInternalAnnotationKey,
		"kubectl.kubernetes.io/last-applied-configuration",
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
	// - Exact annotation keys (e.g., ModelInitInjectionKey) - matches only that specific annotation
	//
	// Both styles work correctly because IsPrefixSupported uses strings.HasPrefix for matching.
	PodOnlyAnnotationPrefixes = []string{
		"k8s.grafana.com/",           // Grafana scraping annotations (k8s.grafana.com/scrape, k8s.grafana.com/port)
		"loki.grafana.com/",          // Loki log collection annotations (loki.grafana.com/scrape, loki.grafana.com/log-format)
		"prometheus.io/",             // Prometheus scraping annotations (prometheus.io/scrape, prometheus.io/port, prometheus.io/path)
		"networking.gke.io/",         // GKE multi-NIC and RDMA network annotations (networking.gke.io/interfaces, etc.)
		"rdma.ome.io/",               // OME RDMA injection annotations (RDMAAutoInjectAnnotationKey, RDMAProfileAnnotationKey, etc.)
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
	Serverless        DeploymentModeType = "Serverless"
	RawDeployment     DeploymentModeType = "RawDeployment"
	MultiNodeRayVLLM  DeploymentModeType = "MultiNodeRayVLLM"
	PDDisaggregated   DeploymentModeType = "PDDisaggregated"
	MultiNode         DeploymentModeType = "MultiNode"
	VirtualDeployment DeploymentModeType = "VirtualDeployment"
)

// IsValid checks if the deployment mode is valid
func (d DeploymentModeType) IsValid() bool {
	switch d {
	case Serverless, RawDeployment, MultiNodeRayVLLM, MultiNode, VirtualDeployment:
		return true
	default:
		return false
	}
}

const (
	DefaultNSKnativeServing = "knative-serving"
)

// revision label
const (
	RevisionLabel         = "serving.knative.dev/revision"
	RawDeploymentAppLabel = "app"
)

// container state reason
const (
	StateReasonRunning          = "Running"
	StateReasonCompleted        = "Completed"
	StateReasonError            = "Error"
	StateReasonCrashLoopBackOff = "CrashLoopBackOff"
)

// Container log / termination message parsing
var (
	AnsiEscapeCodePattern = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)
)

const (
	NotEnoughGPUMemoryMessage     = "not enough gpu memory"
	CUDAOutOfMemoryMessage        = "cuda out of memory"
	ModelLoadGPUOOMFailureMessage = "Model failed to load: not enough GPU memory"
	InsufficientGPUMemoryReason   = "InsufficientGPUMemory"
	InsufficientGPUMemoryMessage  = "Insufficient GPU memory to run the model container"
)

// CRD Kinds
const (
	IstioVirtualServiceKind = "VirtualService"
	KnativeServiceKind      = "Service"
	RayClusterKind          = "RayCluster"
	VolcanoQueueKind        = "Queue"
	KEDAScaledObjectKind    = "ScaledObject"
	VolcanoJobKind          = "Job"
	LWSKind                 = "LeaderWorkerSet"
	GatewayKind             = "Gateway"
	ServiceKind             = "Service"
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

var (
// JobCompletionIndexFieldPath is the field path for the Job completion index annotation.
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

// GetRawServiceLabel generate native service label
func GetRawServiceLabel(service string) string {
	return TruncateNameWithMaxLength(service, 63)
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

func LWSName(isvcName string) string {
	var maxLen = 50
	if len(isvcName) > maxLen {
		isvcName = isvcName[len(isvcName)-maxLen:]
	}
	return fmt.Sprintf("lws-%s", isvcName)
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

func DefaultRayHeadServiceName(name string, index int) string {
	return rayutils.CheckName(fmt.Sprintf("%s-%d", name, index))
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

// truncateModelName truncates a model name to fit within the given constraints
func truncateModelName(modelName string, maxLength int) string {
	return truncateWithHash(modelName, maxLength)
}

// truncateNamespace truncates a namespace name to fit within the given constraints
func truncateNamespace(namespace string, maxLength int) string {
	return truncateWithHash(namespace, maxLength)
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
	truncatedModelName := truncateModelName(modelName, maxModelNameLength)
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

	truncatedNamespace := truncateNamespace(namespace, namespaceMaxLength)
	truncatedModelName := truncateModelName(modelName, modelNameMaxLength)

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
		truncatedModelName := truncateModelName(modelName, maxModelNameLength)
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

	truncatedNamespace := truncateNamespace(namespace, namespaceMaxLength)
	truncatedModelName := truncateModelName(modelName, modelNameMaxLength)

	return fmt.Sprintf("%s.%s.%s", truncatedNamespace, BaseModelLabelType, truncatedModelName)
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
