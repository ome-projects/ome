package controllerconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"text/template"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"golang.org/x/sync/singleflight"

	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	omevalidation "sigs.k8s.io/ome/pkg/validation"
)

const (
	IngressConfigKeyName           = "ingress"
	DeployConfigName               = "deploy"
	OmeAgentConfigName             = "omeAgent"
	CanaryAnalysisConfigName       = "canaryAnalysis"
	CoordinationConfigName         = "coordination"
	PodMonitorConfigName           = "podMonitor"
	LifecycleConfigName            = "lifecycle"
	PodDisruptionBudgetConfigName  = "podDisruptionBudget"
	AcceleratorResourcesConfigName = "acceleratorResources"

	DefaultDomainTemplate = "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}"
	DefaultIngressDomain  = "example.com"

	DefaultUrlScheme = "http"

	// Operational fallbacks for the canary metric-analysis sampler, applied by the
	// loader only when the ConfigMap omits them (the chart sets the real values).
	// These tune the sampler, not deployment identity, so an in-loader fallback is
	// appropriate (mirroring DefaultDomainTemplate); the source address has no such
	// fallback (see CanaryAnalysisConfig.BundledPrometheusAddress).
	DefaultAnalysisQueryTimeout   = "5s"
	DefaultAnalysisCacheTTL       = "2m"
	DefaultAnalysisMaxConcurrency = 8
)

var DefaultConsistentHashHeaders = []string{"x-routing-key"}

type SecretConfig struct {
	WriteToCommonNamespace bool   `json:"writeToCommonNamespace"`
	Namespace              string `json:"namespace"`
	SecretName             string `json:"secretName"`
}

// +kubebuilder:object:generate=false
type InferenceServicesConfig struct {
	// PodMonitor carries cluster-scope defaults applied to every generated
	// PodMonitor (metadata labels + endpoint relabelings). Loaded from the
	// "podMonitor" key of the inferenceservice-config ConfigMap.
	PodMonitor PodMonitorConfig `json:"podMonitor,omitempty"`
	// PodDisruptionBudget carries per-deployment-mode disruption budget policy.
	PodDisruptionBudget PodDisruptionBudgetConfig `json:"podDisruptionBudget,omitempty"`
	// AcceleratorResources are the extended resource names OME recognizes as
	// GPU/accelerator capacity when inspecting a container's resources (e.g.
	// utils.IsGPUEnabled, isvcutils.GetGpuCountFromContainer). Exact names, no
	// patterns — mirrors quota/capacity.Options.AcceleratorResources, e.g.
	// "nvidia.com/gpu", "amd.com/gpu", "google.com/tpu".
	//
	// There is intentionally NO in-code default list — the value is supplied via
	// the inferenceservice-config ConfigMap's "acceleratorResources" key (Helm
	// chart / GitOps). Unlike quota/capacity (where empty disables derivation),
	// empty/absent here does NOT disable accelerator recognition: these helpers
	// existed before this field did and always recognized
	// constants.NvidiaGPUResourceType, so silently switching to "recognize
	// nothing" on upgrade would break every existing nvidia-only cluster.
	// AcceleratorResourceNames applies that documented fallback and should be
	// used instead of reading this field directly.
	AcceleratorResources []string `json:"acceleratorResources,omitempty"`
}

// AcceleratorResourceNames returns the accelerator resource names OME
// recognizes when inspecting a container's resources, applying the
// unconfigured fallback documented on AcceleratorResources. Safe to call on a
// nil receiver.
func (c *InferenceServicesConfig) AcceleratorResourceNames() []string {
	if c == nil || len(c.AcceleratorResources) == 0 {
		return []string{constants.NvidiaGPUResourceType}
	}
	return c.AcceleratorResources
}

// PodDisruptionBudgetPolicy configures one deployment mode's availability
// budget. A configured policy requires exactly one field to be set.
type PodDisruptionBudgetPolicy struct {
	MinAvailable   *intstr.IntOrString `json:"minAvailable,omitempty"`
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// PodDisruptionBudgetConfig contains optional disruption budget policies for
// deployment modes that generate PodDisruptionBudgets.
type PodDisruptionBudgetConfig struct {
	RawDeployment *PodDisruptionBudgetPolicy `json:"rawDeployment,omitempty"`
	OMENative     *PodDisruptionBudgetPolicy `json:"omeNative,omitempty"`
}

// PodMonitorConfig configures the PodMonitor objects OME generates for each
// InferenceService Component. Both fields have NO in-code default — they are
// supplied via the inferenceservice-config ConfigMap (Helm chart / GitOps), so
// scrape-pipeline-specific values are never hardcoded magic strings.
//
// +kubebuilder:object:generate=false
type PodMonitorConfig struct {
	// Labels are merged into each generated PodMonitor's metadata.labels (NOT
	// its spec.selector, which continues to select pods by the component's own
	// labels). Use this to opt the PodMonitor into an external scrape pipeline
	// that selects monitors by label — e.g. an OpenTelemetry target
	// allocator whose podMonitorSelector is {scrape.example.com/tier: cluster}.
	// Empty leaves the PodMonitor with only OME's own labels, in which case a
	// label-selecting collector will not scrape it.
	Labels map[string]string `json:"labels,omitempty"`
	// Relabelings are appended to every PodMetricsEndpoint of every generated
	// PodMonitor. They map Kubernetes service-discovery meta labels onto the
	// stable metric labels dashboards key on (e.g.
	// __meta_kubernetes_pod_label_ome_io_inferenceservice -> inferenceservice),
	// so PodMonitor-scraped series carry the same label schema as the legacy
	// pod-annotation ("kubernetes-pods") scrape path. Empty means OME adds no
	// relabelings and the collector's own defaults apply.
	Relabelings []RelabelConfig `json:"relabelings,omitempty"`
	// MetricRelabelings are appended to every PodMetricsEndpoint's
	// metricRelabelings (Prometheus metric_relabel_configs, applied AFTER the
	// scrape on each sample). Unlike Relabelings — which run before the scrape on
	// the target's label set and cannot see a sample's __name__ — these can
	// rewrite the metric name itself. Use this to normalize engine metric names,
	// e.g. a router that re-exports vLLM metrics under underscore names (vllm_*)
	// back to the colon form (vllm:*) that dashboards query. Empty means OME adds
	// no metric relabelings. Safe cluster-wide when the regex is scoped (e.g.
	// "vllm_(.+)"): it no-ops on series that don't match.
	MetricRelabelings []RelabelConfig `json:"metricRelabelings,omitempty"`
}

// RelabelConfig is a JSON-friendly subset of the Prometheus-operator
// monitoringv1.RelabelConfig. It is converted to the operator type by the
// podmonitor reconciler. Only the fields OME needs are exposed; extend as
// required.
//
// +kubebuilder:object:generate=false
type RelabelConfig struct {
	SourceLabels []string `json:"sourceLabels,omitempty"`
	Separator    string   `json:"separator,omitempty"`
	TargetLabel  string   `json:"targetLabel,omitempty"`
	Regex        string   `json:"regex,omitempty"`
	Replacement  string   `json:"replacement,omitempty"`
	Action       string   `json:"action,omitempty"`
}

// +kubebuilder:object:generate=false
type IngressConfig struct {
	IngressGateway     string `json:"ingressGateway,omitempty"`
	IngressServiceName string `json:"ingressService,omitempty"`
	OmeIngressGateway  string `json:"omeIngressGateway,omitempty"`
	// OmeIngressGatewayClass labels the primary (OmeIngressGateway) gateway's
	// endpoints in status ("internal", "external", ...) — the analogue of
	// IngressGatewaySpec.Class for the cluster-default primary gateway. Free-form;
	// NO in-code default (config-driven per the no-hardcoded-magic-values
	// constraint). Must match the RFC-1123 label form so it is safe as a
	// selector / metrics / path token; invalid values are rejected at config-load.
	// Empty ⇒ the primary endpoint carries no class.
	OmeIngressGatewayClass   string    `json:"omeIngressGatewayClass,omitempty"`
	IngressDomain            string    `json:"ingressDomain,omitempty"`
	IngressClassName         *string   `json:"ingressClassName,omitempty"`
	AdditionalIngressDomains *[]string `json:"additionalIngressDomains,omitempty"`
	DomainTemplate           string    `json:"domainTemplate,omitempty"`
	UrlScheme                string    `json:"urlScheme,omitempty"`
	PathTemplate             string    `json:"pathTemplate,omitempty"`
	DisableIstioVirtualHost  bool      `json:"disableIstioVirtualHost,omitempty"`
	DisableIngressCreation   bool      `json:"disableIngressCreation,omitempty"`
	EnableGatewayAPI         bool      `json:"enableGatewayAPI,omitempty"`
	ConsistentHashHeaders    []string  `json:"consistentHashHeaders,omitempty"`
	// PerISVCSubdomain opts into a per-ISVC subdomain ingress scheme:
	// each ISVC is reached at its own host (matching status.url, rendered
	// from DomainTemplate) with a root path, instead of the default shared
	// "<SharedHostPrefix>.<IngressDomain>" host plus a "/<namespace>/<service>/"
	// path prefix. Default false preserves the shared-host scheme.
	PerISVCSubdomain bool `json:"perISVCSubdomain,omitempty"`
	// SharedHostPrefix is the subdomain label prepended to IngressDomain in the
	// shared-host scheme (PerISVCSubdomain=false): "<SharedHostPrefix>.<IngressDomain>".
	// There is intentionally NO in-code default — the value is supplied via the
	// inferenceservice-config ConfigMap (set by the Helm chart / GitOps), so the
	// host label is never a hardcoded magic string. Empty yields the bare
	// IngressDomain (no prefix).
	SharedHostPrefix string `json:"sharedHostPrefix,omitempty"`
	// AdditionalIngressGateways attaches each generated HTTPRoute to extra
	// gateways beyond the primary OmeIngressGateway — e.g. an external gateway
	// alongside the internal one. Each entry contributes its own parentRef and a
	// hostname rendered from its own IngressDomain. Empty preserves the
	// single-gateway behavior.
	AdditionalIngressGateways []IngressGatewaySpec `json:"additionalIngressGateways,omitempty"`
	// DefaultRouteTimeoutSeconds is the cluster-default Gateway API request
	// timeout (HTTPRouteRule.Timeouts.Request) applied to every generated route
	// whose ISVC does not set a per-component spec.<component>.timeoutSeconds
	// override. There is intentionally NO in-code default — the value is supplied
	// via the inferenceservice-config ConfigMap (Helm chart / GitOps), so the
	// timeout is never a hardcoded magic number. nil or non-positive means OME
	// imposes no route timeout and the gateway's own default applies.
	DefaultRouteTimeoutSeconds *int64 `json:"defaultRouteTimeoutSeconds,omitempty"`
	// NamespaceIngressGateways maps an ISVC's namespace to the gateway(s) its
	// generated HTTPRoutes attach to, overriding the cluster-default primary
	// gateway (OmeIngressGateway/IngressDomain) and AdditionalIngressGateways for
	// ISVCs in that namespace. Namespaces absent from the map fall back to the
	// cluster defaults. This lets one OME controller serve per-namespace Gateways
	// (each with its own TLS cert and DNS, e.g. "prod-int-gw-https" for the "prod"
	// namespace) while the route hostname is unchanged — it is still rendered from
	// DomainTemplate, which already embeds the namespace ("<isvc>.<namespace>.<domain>").
	// A per-ISVC "ome.io/ingress-gateway" (or "ome.io/ingress-additional-gateways")
	// annotation still takes precedence over this namespace default.
	NamespaceIngressGateways map[string]NamespaceIngressGateway `json:"namespaceIngressGateways,omitempty"`
}

// IngressGatewaySpec identifies one gateway an HTTPRoute attaches to, plus the
// domain used to build that gateway's hostname for the route.
type IngressGatewaySpec struct {
	// OmeIngressGateway is the gateway parentRef in "namespace/name" form.
	OmeIngressGateway string `json:"omeIngressGateway"`
	// IngressDomain is the domain used to render this gateway's route hostname
	// (same scheme as the primary: per-ISVC subdomain or shared-host prefix).
	IngressDomain string `json:"ingressDomain"`
	// Class labels this gateway's endpoints in status ("internal", "external",
	// "exp", ...). Free-form; NO in-code default (config-driven per the
	// no-hardcoded-magic-values constraint). Must match the RFC-1123 label form
	// [a-z0-9]([-a-z0-9]*[a-z0-9])? so it is safe as a selector / metrics / path
	// token; invalid values are rejected at config-load. Empty ⇒ the endpoint
	// carries no class.
	Class string `json:"class,omitempty"`
}

// NamespaceIngressGateway is the per-namespace gateway override applied to every
// ISVC in a namespace (keyed by namespace in IngressConfig.NamespaceIngressGateways):
// the primary gateway its HTTPRoutes attach to plus any additional gateways,
// replacing the cluster defaults for that namespace.
type NamespaceIngressGateway struct {
	// Primary is the primary gateway (parentRef "namespace/name") and the domain
	// used to render its route hostname for ISVCs in this namespace. When
	// OmeIngressGateway is empty the cluster-default primary is kept.
	Primary IngressGatewaySpec `json:"primary"`
	// Additional attaches each generated HTTPRoute to extra gateways beyond the
	// primary (e.g. an external gateway alongside the internal one), replacing the
	// cluster-default AdditionalIngressGateways for this namespace. nil keeps the
	// cluster default; an explicit empty list means "no additional gateways".
	Additional []IngressGatewaySpec `json:"additional,omitempty"`
}

// +kubebuilder:object:generate=false
type DeployConfig struct {
	DefaultDeploymentMode string `json:"defaultDeploymentMode,omitempty"`
	// Replicas carries the admission-time component replica defaults the
	// ISVC defaulter stamps on unset minReplicas/maxReplicas fields. There
	// are intentionally NO in-code defaults — the values are supplied via
	// the inferenceservice-config ConfigMap (Helm chart / GitOps). Absent
	// (nil block or nil field) means unconfigured: the defaulter leaves the
	// corresponding field as authored, never a silent fallback to baked-in
	// numbers. Configured values must be > 0 (rejected at config-load).
	Replicas *ReplicasDefaultsConfig `json:"replicas,omitempty"`
	// TerminationGracePeriodSeconds is the admission-time pod termination
	// grace the ISVC defaulter stamps on any component that does not author
	// one. It bounds how long a component has to finish in-flight work after
	// SIGTERM, so a serving component whose requests outlive the platform
	// default loses them on every restart unless this is raised.
	//
	// Following Replicas above, there is no in-code default: nil means
	// unconfigured and the component keeps whatever it authored. A configured
	// value must be > 0 (rejected at config-load).
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// MinReadySeconds is the admission-time lifecycle.minReadySeconds the
	// ISVC defaulter stamps on every OMENative component that does not
	// author one: the time a newly Ready pod must stay Ready before a
	// rollout drains or promotes past it. No in-code default: nil means
	// unconfigured and the component keeps whatever it authored (unset =
	// Available as soon as Ready). A configured value must be >= 0.
	MinReadySeconds *int32 `json:"minReadySeconds,omitempty"`
	// UpdateStrategy is the admission-time lifecycle.updateStrategy the ISVC
	// defaulter stamps on OMENative components that do not author one, keyed
	// by component so a router and an engine can roll differently. Following
	// the fields above there is no in-code default: an absent block, an
	// absent component entry, or an absent field means unconfigured and the
	// component keeps whatever it authored.
	UpdateStrategy *UpdateStrategyDefaultsConfig `json:"updateStrategy,omitempty"`
}

// UpdateStrategyDefaultsConfig is the admission-time per-pod-swap defaulting
// policy loaded from the "deploy.updateStrategy" block of the
// inferenceservice-config ConfigMap. Each component is independent; a nil
// entry disables defaulting for that component only.
//
// +kubebuilder:object:generate=false
type UpdateStrategyDefaultsConfig struct {
	Router  *ComponentUpdateStrategyDefaults `json:"router,omitempty"`
	Engine  *ComponentUpdateStrategyDefaults `json:"engine,omitempty"`
	Decoder *ComponentUpdateStrategyDefaults `json:"decoder,omitempty"`
}

// ComponentUpdateStrategyDefaults carries one component's per-pod-swap
// defaults. Both budgets may be configured; the defaulter stamps only the one
// the resolved strategy reads, because a surge strategy never consults
// MaxUnavailable and a non-surge strategy never consults MaxSurge, and a value
// that is never read reads as a bound that is doing nothing.
//
// +kubebuilder:object:generate=false
type ComponentUpdateStrategyDefaults struct {
	// Type is the update strategy, one of the values the UpdateStrategyType
	// enum accepts.
	Type string `json:"type,omitempty"`
	// MaxSurge paces a surge strategy: extra Instances allowed above the
	// component's replica count during a roll.
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`
	// MaxUnavailable paces a non-surge strategy: Instances allowed to be
	// not-Ready at once during a roll.
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// ForComponent returns the entry for a component, or nil when the block or the
// entry is unconfigured.
func (c *UpdateStrategyDefaultsConfig) ForComponent(component workloadtypes.ComponentType) *ComponentUpdateStrategyDefaults {
	if c == nil {
		return nil
	}
	switch component {
	case workloadtypes.ComponentRouter:
		return c.Router
	case workloadtypes.ComponentEngine:
		return c.Engine
	case workloadtypes.ComponentDecoder:
		return c.Decoder
	default:
		return nil
	}
}

// validate rejects a malformed block at config-load rather than letting the
// defaulter stamp a strategy the CRD enum refuses or a budget that deadlocks
// every rollout.
func (c *UpdateStrategyDefaultsConfig) validate() error {
	if c == nil {
		return nil
	}
	for _, entry := range []struct {
		name  string
		value *ComponentUpdateStrategyDefaults
	}{{"router", c.Router}, {"engine", c.Engine}, {"decoder", c.Decoder}} {
		if entry.value == nil {
			continue
		}
		if err := entry.value.validate(entry.name); err != nil {
			return err
		}
	}
	return nil
}

func (e *ComponentUpdateStrategyDefaults) validate(component string) error {
	switch workloadtypes.UpdateStrategyType(e.Type) {
	case "",
		workloadtypes.UpdateStrategySurgeThenDrain,
		workloadtypes.UpdateStrategyRecreatePod,
		workloadtypes.UpdateStrategyInPlaceIfPossible,
		workloadtypes.UpdateStrategyInPlaceOnly:
	default:
		return fmt.Errorf("invalid deploy config, updateStrategy.%s.type %q is not a supported update strategy", component, e.Type)
	}
	for _, budget := range []struct {
		field string
		value *intstr.IntOrString
	}{{"maxSurge", e.MaxSurge}, {"maxUnavailable", e.MaxUnavailable}} {
		if budget.value == nil {
			continue
		}
		scaled, err := intstr.GetScaledValueFromIntOrPercent(budget.value, 100, true)
		if err != nil {
			return fmt.Errorf("invalid deploy config, updateStrategy.%s.%s is not an integer or percentage: %w", component, budget.field, err)
		}
		if scaled <= 0 {
			return fmt.Errorf("invalid deploy config, updateStrategy.%s.%s must be > 0, got %q: a zero budget denies every Instance and the rollout never starts", component, budget.field, budget.value.String())
		}
	}
	return nil
}

// ReplicasDefaultsConfig is the admission-time replica-defaulting policy
// loaded from the "deploy.replicas" block of the inferenceservice-config
// ConfigMap. Every field is optional; a nil field disables defaulting of
// that value only.
//
// +kubebuilder:object:generate=false
type ReplicasDefaultsConfig struct {
	// DefaultMinReplicas fills an unset component minReplicas (all
	// components share one floor default).
	DefaultMinReplicas *int `json:"defaultMinReplicas,omitempty"`
	// DefaultMaxReplicas fills an unset component maxReplicas, per
	// component. The defaulter raises the filled value to an authored
	// minReplicas so it never manufactures a min>max conflict.
	DefaultMaxReplicas ComponentMaxReplicasDefaults `json:"defaultMaxReplicas,omitempty"`
}

// ComponentMaxReplicasDefaults holds the per-component maxReplicas defaults.
//
// +kubebuilder:object:generate=false
type ComponentMaxReplicasDefaults struct {
	Engine  *int `json:"engine,omitempty"`
	Decoder *int `json:"decoder,omitempty"`
	Router  *int `json:"router,omitempty"`
}

// Min returns the configured minReplicas default; nil (including a nil
// receiver) means unconfigured — the defaulter leaves the field as authored.
func (c *ReplicasDefaultsConfig) Min() *int {
	if c == nil {
		return nil
	}
	return c.DefaultMinReplicas
}

// EngineMax returns the configured engine maxReplicas default; nil
// (including a nil receiver) means unconfigured.
func (c *ReplicasDefaultsConfig) EngineMax() *int {
	if c == nil {
		return nil
	}
	return c.DefaultMaxReplicas.Engine
}

// DecoderMax returns the configured decoder maxReplicas default; nil
// (including a nil receiver) means unconfigured.
func (c *ReplicasDefaultsConfig) DecoderMax() *int {
	if c == nil {
		return nil
	}
	return c.DefaultMaxReplicas.Decoder
}

// RouterMax returns the configured router maxReplicas default; nil
// (including a nil receiver) means unconfigured.
func (c *ReplicasDefaultsConfig) RouterMax() *int {
	if c == nil {
		return nil
	}
	return c.DefaultMaxReplicas.Router
}

// validate rejects non-positive configured values. A replica default of
// zero or below can never be a sane admission stamp, so it is a config
// error surfaced at load time rather than a bad object stored later.
func (c *ReplicasDefaultsConfig) validate() error {
	fields := []struct {
		name  string
		value *int
	}{
		{"defaultMinReplicas", c.DefaultMinReplicas},
		{"defaultMaxReplicas.engine", c.DefaultMaxReplicas.Engine},
		{"defaultMaxReplicas.decoder", c.DefaultMaxReplicas.Decoder},
		{"defaultMaxReplicas.router", c.DefaultMaxReplicas.Router},
	}
	for _, f := range fields {
		if f.value != nil && *f.value <= 0 {
			return fmt.Errorf("invalid deploy config, replicas.%s must be > 0, got %d", f.name, *f.value)
		}
	}
	return nil
}

// +kubebuilder:object:generate=false
// CanaryAnalysisConfig holds operator-level defaults for metric-gated canary
// promotion: the default Prometheus source and the sampler's tuning. A per-CR
// spec.rollout.canary.prometheus.serverAddress overrides BundledPrometheusAddress.
type CanaryAnalysisConfig struct {
	// BundledPrometheusAddress is the metrics source queried when a canary does not
	// set its own spec.rollout.canary.prometheus.serverAddress. There is
	// intentionally NO in-code default — the value is supplied via the
	// inferenceservice-config ConfigMap (set by the Helm chart / GitOps), so the
	// address is never a hardcoded magic string. Empty means a canary with no
	// per-CR source has no source, and its samples read as inconclusive — it does
	// not silently fall back to a baked-in address.
	BundledPrometheusAddress string `json:"bundledPrometheusAddress,omitempty"`
	// DefaultProvider names a metricProviders binding used as the analysis
	// source when a CR names neither a providerRef nor its own serverAddress.
	// Empty keeps the BundledPrometheusAddress fallback. A name with no
	// binding is a deterministic config error surfaced by the consumer when a
	// run opens — never a silent fallback.
	DefaultProvider string `json:"defaultProvider,omitempty"`
	// QueryTimeout bounds one sampling pass (all of a step's metric queries). A
	// duration string (e.g. "5s"); the loader falls back to DefaultAnalysisQueryTimeout.
	QueryTimeout string `json:"queryTimeout,omitempty"`
	// MaxConcurrency caps how many analysis queries run in the background at once,
	// across all canaries. It is a fleet-wide ceiling: with many concurrent canaries
	// queries queue behind it and the effective sample interval degrades (watch the
	// ome_canary_sampler_queue_depth / _starved_total metrics), so scale this with
	// fleet size. The loader falls back to DefaultAnalysisMaxConcurrency.
	MaxConcurrency int32 `json:"maxConcurrency,omitempty"`
	// CacheTTL is how long a sampled result stays usable before the sampler evicts
	// it. A duration string; the loader falls back to DefaultAnalysisCacheTTL.
	CacheTTL string `json:"cacheTTL,omitempty"`
}

// +kubebuilder:object:generate=false
// CoordinationConfig holds operator-level tuning for the OMENative cross-Component
// coordination layer (the pod-proportional per-revision traffic producer).
type CoordinationConfig struct {
	// TrafficWeightDeadbandPercent is the per-revision hysteresis band applied to
	// the pod-proportional traffic writer: a recomputed weight set whose every
	// per-revision percent moves by strictly less than this from what is already in
	// Status.Components.<c>.Traffic is treated as pod-count jitter and the status
	// write (plus the HTTPRoute re-enqueue it triggers) is suppressed. Dampens the
	// continuous churn from pods momentarily Pending between reconciles at scale.
	//
	// There is intentionally NO in-code default — the value is supplied via the
	// inferenceservice-config ConfigMap (set by the Helm chart / GitOps). Zero
	// (absent config) disables the band and preserves the exact prior
	// write-on-any-diff behavior, so eventual correctness is never traded away
	// silently.
	TrafficWeightDeadbandPercent int32 `json:"trafficWeightDeadbandPercent,omitempty"`

	// DefaultRatioTolerancePercent fills spec.rollout.groups[].maintainRatio
	// .tolerance when a group sets maintainRatio but omits the tolerance. An
	// explicit per-group tolerance (including 0) always wins. There is
	// intentionally NO in-code default — the value is supplied via the
	// inferenceservice-config ConfigMap (set by the Helm chart / GitOps).
	// Nil (absent config) means unconfigured: a group that omits tolerance
	// then rolls with no drift bound, never a silently baked-in number.
	DefaultRatioTolerancePercent *int32 `json:"defaultRatioTolerancePercent,omitempty"`
}

// +kubebuilder:object:generate=false
// LifecycleConfig holds operator-level tuning for the OMENative per-Instance
// lifecycle state machine, loaded from the "lifecycle" key of the
// inferenceservice-config ConfigMap.
type LifecycleConfig struct {
	// UpdateRetry bounds automatic same-target update retries (RetryBlock).
	// There are intentionally NO in-code defaults — the values are supplied
	// via the inferenceservice-config ConfigMap (Helm chart / GitOps). Absent
	// means unconfigured: the workload layer fails safe (the first same-target
	// failure Holds), never a silent fallback to baked-in numbers.
	UpdateRetry *UpdateRetryConfig `json:"updateRetry,omitempty"`
	// StuckPodGracePeriod is the wait window after pod creation before a
	// terminal kubelet waiting state escalates to Phase=Failed. A duration
	// string ("60s"); absence or parse failure disables fast escalation
	// this pass (the InstanceReadyTimeout backstop remains).
	StuckPodGracePeriod string `json:"stuckPodGracePeriod,omitempty"`
	// AutoMigrate configures the deadline-disposition relocation budget.
	// Absence disables the relocation branch.
	AutoMigrate *AutoMigrateConfig `json:"autoMigrate,omitempty"`
	// ForceDelete configures the stuck-Terminating force-delete
	// escalation. Absence means the escalation does not exist — there
	// are intentionally NO in-code defaults.
	ForceDelete *ForceDeleteConfig `json:"forceDelete,omitempty"`
	// Teardown configures the finalizer-gated IR teardown deadline.
	// Absence means the finalizer holds strictly until clean (NO
	// deadline, the fully supported default). When present, Deadline is
	// REQUIRED and bounds how long the finalizer holds.
	Teardown *TeardownConfig `json:"teardown,omitempty"`
	// ScaleUpPodBatchSize bounds missing-Pod create selection in Pod units.
	// An Instance (including a leader/worker gang) remains indivisible: all of
	// its missing Pods are selected together and its durable Creating intent is
	// committed before any corresponding Pod create is issued. The first
	// eligible Instance may exceed a positive budget and proceeds alone. Nil
	// preserves this field's unbounded compatibility behavior.
	ScaleUpPodBatchSize *int32 `json:"scaleUpPodBatchSize,omitempty"`
	// ScaleDownPodBatchSize bounds active delete selection in Pod-equivalent
	// units: its live and Terminating Pods, with a cost-1 floor for Podless
	// status work. An Instance (including a leader/worker gang) remains
	// indivisible. The first eligible Instance may exceed a positive budget and
	// proceeds alone. Nil preserves this field's unbounded compatibility behavior.
	ScaleDownPodBatchSize *int32 `json:"scaleDownPodBatchSize,omitempty"`
	// ScaleDownRequeueInterval is the periodic wake-up cadence while destructive
	// work remains in flight. The controller also watches Pods, EndpointSlices,
	// PodGroups, and its owner. Absence disables only cadence polling; configured
	// force-delete and teardown deadlines still schedule their exact wake-ups.
	// There is intentionally no in-code default.
	ScaleDownRequeueInterval *string `json:"scaleDownRequeueInterval,omitempty"`
	// RevisionHistoryLimit is the operator-level cap on non-live
	// ControllerRevisions retained per InferenceReplica when the parent
	// InferenceService does not set the ome.io/revision-history-limit
	// annotation. Live revisions are never deleted regardless of the cap.
	// There is intentionally NO in-code default — the value is supplied via
	// the inferenceservice-config ConfigMap (Helm chart / GitOps). Absent
	// means unconfigured: without a per-ISVC annotation the retention sweep
	// prunes nothing, never a silent fallback to a baked-in number.
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty"`
}

// PodBatchSizes contains the process-scoped OMENative scale settings loaded
// from one lifecycle ConfigMap snapshot. Nil batch fields preserve that
// direction's unbounded compatibility behavior; a zero requeue interval leaves
// progress to watched resources and exact configured lifecycle deadlines.
//
// +kubebuilder:object:generate=false
type PodBatchSizes struct {
	ScaleUp                  *int32
	ScaleDown                *int32
	ScaleDownRequeueInterval time.Duration
}

// UpdateRetryConfig is the same-target update retry policy: a failed rollout
// toward an unchanged target revision backs off exponentially
// (initialDelay * multiplier^(attempt-1), capped at maxDelay) and Holds after
// maxAttempts. Delays are duration strings ("1m", "30m").
//
// +kubebuilder:object:generate=false
type UpdateRetryConfig struct {
	MaxAttempts  int32   `json:"maxAttempts"`
	InitialDelay string  `json:"initialDelay"`
	MaxDelay     string  `json:"maxDelay"`
	Multiplier   float64 `json:"multiplier"`
}

// AutoMigrateConfig configures the deadline-disposition relocation budget.
//
// +kubebuilder:object:generate=false
type AutoMigrateConfig struct {
	// MaxAttempts bounds the number of relocation attempts for one expired
	// Instance before the disposition falls through to terminal failure.
	MaxAttempts int32 `json:"maxAttempts"`
}

// ForceDeleteConfig configures the stuck-Terminating force-delete
// escalation. Both fields are duration strings ("2m", "5m") and are
// REQUIRED when the block is present — there are no in-code defaults.
//
// +kubebuilder:object:generate=false
type ForceDeleteConfig struct {
	// OverdueSlack is how long past a Terminating pod's own
	// deletionTimestamp (request time + the pod's own grace period) the
	// pod must be before it counts as wedged.
	OverdueSlack string `json:"overdueSlack"`
	// NodeUnreachableThreshold is the minimum age of the node's
	// unreachable evidence (taint TimeAdded / NotReady
	// LastTransitionTime, or the Node object gone) before the
	// escalation may act.
	NodeUnreachableThreshold string `json:"nodeUnreachableThreshold"`
}

// TeardownConfig configures the finalizer-gated IR teardown deadline.
// The Deadline field is a duration string ("30m") and is REQUIRED when
// the block is present — there is no in-code default.
//
// +kubebuilder:object:generate=false
type TeardownConfig struct {
	// Deadline bounds how long a deleting InferenceReplica may hold its
	// finalizer while draining pods. Past it, the finalizer warns and
	// lifts (degrading to plain GC). Absence (of the entire block) is
	// the fully supported default: the finalizer holds strictly until
	// clean, NO deadline.
	Deadline string `json:"deadline"`
}

// ToDeadline validates the config and converts Deadline to a
// *time.Duration. A nil receiver (absent block) yields (nil, nil) —
// unconfigured, no deadline, the finalizer holds strictly until clean
// (fully supported). Empty, unparsable, or non-positive Deadline is an
// error; callers treat invalid config as unconfigured (no deadline).
func (c *TeardownConfig) ToDeadline() (*time.Duration, error) {
	if c == nil {
		return nil, nil
	}
	if c.Deadline == "" {
		return nil, fmt.Errorf("invalid lifecycle.teardown: deadline is required when the block is present")
	}
	d, err := time.ParseDuration(c.Deadline)
	if err != nil {
		return nil, fmt.Errorf("invalid lifecycle.teardown: deadline %q: %w", c.Deadline, err)
	}
	if d <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.teardown: deadline must be > 0, got %s", d)
	}
	return &d, nil
}

// ToPolicy validates the config and converts it to the workload-side
// ForceDeletePolicy. A nil receiver (absent block) yields (nil, nil) —
// unconfigured, the escalation is disabled. Any violation — either
// field missing, unparsable, or non-positive — is an error; callers
// treat an invalid policy as unconfigured (escalation OFF), never patch
// it up with fallback numbers.
func (c *ForceDeleteConfig) ToPolicy() (*workloadtypes.ForceDeletePolicy, error) {
	if c == nil {
		return nil, nil
	}
	overdue, err := time.ParseDuration(c.OverdueSlack)
	if err != nil {
		return nil, fmt.Errorf("invalid lifecycle.forceDelete: overdueSlack %q: %w", c.OverdueSlack, err)
	}
	if overdue <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.forceDelete: overdueSlack must be > 0, got %s", overdue)
	}
	threshold, err := time.ParseDuration(c.NodeUnreachableThreshold)
	if err != nil {
		return nil, fmt.Errorf("invalid lifecycle.forceDelete: nodeUnreachableThreshold %q: %w", c.NodeUnreachableThreshold, err)
	}
	if threshold <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.forceDelete: nodeUnreachableThreshold must be > 0, got %s", threshold)
	}
	return &workloadtypes.ForceDeletePolicy{
		OverdueSlack:             overdue,
		NodeUnreachableThreshold: threshold,
	}, nil
}

// ToPolicy validates the config and converts it to the workload-side
// RetryPolicy. Any violation is an error — callers treat an invalid policy
// as unconfigured (fail-safe Held), never patch it up with fallback numbers.
func (c *UpdateRetryConfig) ToPolicy() (*workloadtypes.RetryPolicy, error) {
	if c.MaxAttempts <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.updateRetry: maxAttempts must be > 0, got %d", c.MaxAttempts)
	}
	if c.Multiplier < 1 {
		return nil, fmt.Errorf("invalid lifecycle.updateRetry: multiplier must be >= 1, got %v", c.Multiplier)
	}
	initial, err := time.ParseDuration(c.InitialDelay)
	if err != nil {
		return nil, fmt.Errorf("invalid lifecycle.updateRetry: initialDelay %q: %w", c.InitialDelay, err)
	}
	maxDelay, err := time.ParseDuration(c.MaxDelay)
	if err != nil {
		return nil, fmt.Errorf("invalid lifecycle.updateRetry: maxDelay %q: %w", c.MaxDelay, err)
	}
	if initial <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.updateRetry: initialDelay must be > 0, got %s", initial)
	}
	if maxDelay <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.updateRetry: maxDelay must be > 0, got %s", maxDelay)
	}
	if maxDelay < initial {
		return nil, fmt.Errorf("invalid lifecycle.updateRetry: maxDelay %s must be >= initialDelay %s", maxDelay, initial)
	}
	return &workloadtypes.RetryPolicy{
		MaxAttempts:  c.MaxAttempts,
		InitialDelay: initial,
		MaxDelay:     maxDelay,
		Multiplier:   c.Multiplier,
	}, nil
}

// ToGracePeriod validates and parses StuckPodGracePeriod. Returns an
// error when the value is malformed or non-positive; absence (empty
// string) is NOT an error (the caller distinguishes unconfigured from
// invalid via the returned zero value).
func (c *LifecycleConfig) ToGracePeriod() (time.Duration, error) {
	if c.StuckPodGracePeriod == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.StuckPodGracePeriod)
	if err != nil {
		return 0, fmt.Errorf("invalid lifecycle.stuckPodGracePeriod %q: %w", c.StuckPodGracePeriod, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("invalid lifecycle.stuckPodGracePeriod: must be > 0, got %s", d)
	}
	return d, nil
}

// Validate checks AutoMigrateConfig constraints.
func (c *AutoMigrateConfig) Validate() error {
	if c.MaxAttempts <= 0 {
		return fmt.Errorf("invalid lifecycle.autoMigrate: maxAttempts must be > 0, got %d", c.MaxAttempts)
	}
	return nil
}

// ToScaleUpPodBatchSize validates the configured missing-Pod batch size. A nil
// LifecycleConfig or absent field means only this field uses its unbounded
// compatibility behavior; it does not fabricate a default. Explicit zero or
// negative values are invalid so manager startup can reject bad configuration.
func (c *LifecycleConfig) ToScaleUpPodBatchSize() (*int32, error) {
	if c == nil {
		return nil, nil
	}
	return positivePodBatchSize("scaleUpPodBatchSize", c.ScaleUpPodBatchSize)
}

// ToScaleDownPodBatchSize validates the configured delete Pod-equivalent batch
// size. A nil LifecycleConfig or absent field preserves unbounded candidate
// selection. Explicit zero or negative values are invalid so manager startup
// can reject bad configuration.
func (c *LifecycleConfig) ToScaleDownPodBatchSize() (*int32, error) {
	if c == nil {
		return nil, nil
	}
	return positivePodBatchSize("scaleDownPodBatchSize", c.ScaleDownPodBatchSize)
}

// ToScaleDownRequeueInterval validates the configured destructive-work polling
// cadence. A nil LifecycleConfig or absent field disables periodic polling
// without disabling exact configured lifecycle-deadline wake-ups or fabricating
// a binary default. Explicit malformed, zero, or negative durations are
// rejected at startup.
func (c *LifecycleConfig) ToScaleDownRequeueInterval() (time.Duration, error) {
	if c == nil || c.ScaleDownRequeueInterval == nil {
		return 0, nil
	}
	interval, err := time.ParseDuration(*c.ScaleDownRequeueInterval)
	if err != nil {
		return 0, fmt.Errorf("invalid lifecycle.scaleDownRequeueInterval %q: %w", *c.ScaleDownRequeueInterval, err)
	}
	if interval <= 0 {
		return 0, fmt.Errorf("invalid lifecycle.scaleDownRequeueInterval: must be > 0, got %s", interval)
	}
	return interval, nil
}

// ToRevisionHistoryLimit validates the configured non-live revision
// retention cap. A nil LifecycleConfig or absent field means
// unconfigured (nil, nil) — the caller prunes nothing rather than
// fabricating a default. Explicit zero or negative values are invalid;
// callers treat invalid config as unconfigured.
func (c *LifecycleConfig) ToRevisionHistoryLimit() (*int32, error) {
	if c == nil || c.RevisionHistoryLimit == nil {
		return nil, nil
	}
	if *c.RevisionHistoryLimit <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.revisionHistoryLimit: must be > 0, got %d", *c.RevisionHistoryLimit)
	}
	limit := *c.RevisionHistoryLimit
	return &limit, nil
}

func positivePodBatchSize(field string, configured *int32) (*int32, error) {
	if configured == nil {
		return nil, nil
	}
	if *configured <= 0 {
		return nil, fmt.Errorf("invalid lifecycle.%s: must be > 0, got %d", field, *configured)
	}
	size := *configured
	return &size, nil
}

// LoadPodBatchSizes reads one lifecycle ConfigMap snapshot and validates the
// process-scoped scale settings. Callers keep the returned values for the
// lifetime of the controller process.
func LoadPodBatchSizes(clientset kubernetes.Interface) (PodBatchSizes, error) {
	lifecycleConfig, err := NewLifecycleConfig(clientset)
	if err != nil {
		return PodBatchSizes{}, fmt.Errorf("load lifecycle configuration: %w", err)
	}
	scaleUp, err := lifecycleConfig.ToScaleUpPodBatchSize()
	if err != nil {
		return PodBatchSizes{}, fmt.Errorf("validate lifecycle.scaleUpPodBatchSize: %w", err)
	}
	scaleDown, err := lifecycleConfig.ToScaleDownPodBatchSize()
	if err != nil {
		return PodBatchSizes{}, fmt.Errorf("validate lifecycle.scaleDownPodBatchSize: %w", err)
	}
	scaleDownRequeueInterval, err := lifecycleConfig.ToScaleDownRequeueInterval()
	if err != nil {
		return PodBatchSizes{}, fmt.Errorf("validate lifecycle.scaleDownRequeueInterval: %w", err)
	}
	return PodBatchSizes{
		ScaleUp:                  scaleUp,
		ScaleDown:                scaleDown,
		ScaleDownRequeueInterval: scaleDownRequeueInterval,
	}, nil
}

// LoadScaleUpPodBatchSize reads and validates only the process-scoped scale-up
// budget. The manager uses LoadPodBatchSizes so both directions share one
// ConfigMap snapshot; this field-specific loader remains available to callers
// that do not consume scale-down configuration.
func LoadScaleUpPodBatchSize(clientset kubernetes.Interface) (*int32, error) {
	lifecycleConfig, err := NewLifecycleConfig(clientset)
	if err != nil {
		return nil, fmt.Errorf("load lifecycle configuration: %w", err)
	}
	size, err := lifecycleConfig.ToScaleUpPodBatchSize()
	if err != nil {
		return nil, fmt.Errorf("validate lifecycle.scaleUpPodBatchSize: %w", err)
	}
	return size, nil
}

// NewLifecycleConfig loads the lifecycle block from the inferenceservice
// ConfigMap. An absent "lifecycle" key yields (nil, nil); each caller applies
// the compatibility or fail-safe semantics of the field it consumes.
func NewLifecycleConfig(clientset kubernetes.Interface) (*LifecycleConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseLifecycleConfig(configMap)
}

// NewLifecycleConfigCached is the ConfigCache-backed variant of
// NewLifecycleConfig used on the reconcile hot path.
func NewLifecycleConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*LifecycleConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseLifecycleConfig(configMap)
}

func parseLifecycleConfig(configMap *v1.ConfigMap) (*LifecycleConfig, error) {
	data, ok := configMap.Data[LifecycleConfigName]
	if !ok || strings.TrimSpace(data) == "" {
		return nil, nil
	}
	cfg := &LifecycleConfig{}
	if err := json.Unmarshal([]byte(data), cfg); err != nil {
		return nil, fmt.Errorf("unable to parse lifecycle config json: %w", err)
	}
	return cfg, nil
}

// +kubebuilder:object:generate=false
// OmeAgentConfig configures the metadata-extraction Job spawned by the
// BaseModel/ClusterBaseModel controller for PVC-backed models. The Job runs
// `ome-agent model-metadata` against the PVC.
type OmeAgentConfig struct {
	// Image is the ome-agent container image. Required.
	Image string `json:"image"`
	// ServiceAccount the metadata Job pod runs as. Must have RBAC to
	// get/list/create/update/patch ConfigMaps in the OME namespace —
	// the agent surfaces extracted metadata via a per-model status
	// ConfigMap, not via direct CR updates.
	ServiceAccount string `json:"serviceAccount,omitempty"`
	// CPURequest/MemoryRequest/CPULimit/MemoryLimit follow the standard
	// resource shape; empty strings fall back to the K8s default.
	CPURequest    string `json:"cpuRequest,omitempty"`
	MemoryRequest string `json:"memoryRequest,omitempty"`
	CPULimit      string `json:"cpuLimit,omitempty"`
	MemoryLimit   string `json:"memoryLimit,omitempty"`
	// BackoffLimit caps Job retries. Default 2 if zero.
	BackoffLimit int32 `json:"backoffLimit,omitempty"`
	// TTLSecondsAfterFinished controls cleanup of completed Jobs.
	// Default 3600 if zero.
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// NodeSelector / Tolerations / Affinity / PriorityClassName are
	// pass-through scheduling hints applied to the metadata Job pod.
	// Necessary when the PVC's CSI driver only mounts on a subset of
	// nodes, or when the cluster taints GPU nodes that hold the models.
	NodeSelector      map[string]string `json:"nodeSelector,omitempty"`
	Tolerations       []v1.Toleration   `json:"tolerations,omitempty"`
	Affinity          *v1.Affinity      `json:"affinity,omitempty"`
	PriorityClassName string            `json:"priorityClassName,omitempty"`
}

// getInferenceServiceConfigMap fetches the inferenceservice-config ConfigMap
// directly from the apiserver via the clientset. This is the single point all
// config constructors below resolve through; a ConfigCache (see below) wraps it
// with a short TTL so a single reconcile pass does not issue one uncached GET
// per config block.
func getInferenceServiceConfigMap(clientset kubernetes.Interface) (*v1.ConfigMap, error) {
	return clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
}

// ConfigCache caches the inferenceservice-config ConfigMap for a configurable
// TTL so the per-reconcile config constructors share one apiserver GET instead
// of issuing one each. The TTL is supplied by the caller (flag/chart-driven —
// no in-code behavioral default); a short TTL preserves the property that a
// ConfigMap edit takes effect without a controller restart, since the entry is
// re-fetched once the TTL elapses.
//
// A zero (or negative) TTL disables caching: every call falls through to the
// apiserver, exactly reproducing the pre-cache behavior.
//
// +kubebuilder:object:generate=false
type ConfigCache struct {
	ttl time.Duration
	now func() time.Time

	// sf collapses concurrent refreshes (under MaxConcurrentReconciles>1) so an
	// expiry triggers exactly one apiserver GET: callers that miss the cache at
	// the same time share the single in-flight fetch instead of each issuing
	// their own. Keyed on the ConfigMap name.
	sf singleflight.Group

	mu        sync.Mutex
	cm        *v1.ConfigMap
	fetchedAt time.Time
}

// NewConfigCache returns a ConfigCache with the given TTL.
func NewConfigCache(ttl time.Duration) *ConfigCache {
	return &ConfigCache{ttl: ttl, now: time.Now}
}

// TTL reports the cache's refresh interval — the upper bound on how stale a
// config-derived decision can be, which controllers use to size periodic
// re-checks of config-driven state. Non-positive (including a nil cache)
// means caching is disabled and every read is live.
func (c *ConfigCache) TTL() time.Duration {
	if c == nil {
		return 0
	}
	return c.ttl
}

// get returns the cached ConfigMap when the entry is fresh, otherwise fetches
// it from the apiserver and refreshes the entry. A non-positive TTL never
// caches. The returned ConfigMap is shared (read-only): the config constructors
// only read .Data, never mutate it.
//
// The lock is held only to check freshness and to swap in a result — never
// across the apiserver GET. Concurrent refreshes after an expiry are collapsed
// via singleflight, so the TTL window costs exactly one apiserver GET no matter
// how many reconciles race into it.
func (c *ConfigCache) get(clientset kubernetes.Interface) (*v1.ConfigMap, error) {
	if c == nil || c.ttl <= 0 {
		return getInferenceServiceConfigMap(clientset)
	}

	// Fast path: serve a fresh entry without touching the apiserver.
	c.mu.Lock()
	if c.cm != nil && c.now().Sub(c.fetchedAt) < c.ttl {
		cm := c.cm
		c.mu.Unlock()
		return cm, nil
	}
	c.mu.Unlock()

	// Miss/expiry: collapse concurrent refreshes onto a single GET. The result
	// is shared with every caller that joins this in-flight fetch.
	v, err, _ := c.sf.Do(constants.InferenceServiceConfigMapName, func() (interface{}, error) {
		// Re-check freshness now that we hold the singleflight slot: a refresh
		// may have completed between our fast-path miss and acquiring it,
		// leaving the entry fresh again (avoids a redundant back-to-back GET).
		c.mu.Lock()
		if c.cm != nil && c.now().Sub(c.fetchedAt) < c.ttl {
			cm := c.cm
			c.mu.Unlock()
			return cm, nil
		}
		c.mu.Unlock()

		// GET happens outside the lock so concurrent readers are never blocked
		// on apiserver I/O.
		cm, err := getInferenceServiceConfigMap(clientset)

		c.mu.Lock()
		defer c.mu.Unlock()
		if err != nil {
			// On error, drop any stale entry so the next call retries rather
			// than serving a config that may no longer exist.
			c.cm = nil
			return nil, err
		}
		c.cm = cm
		c.fetchedAt = c.now()
		return cm, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*v1.ConfigMap), nil
}

// NewInferenceServicesConfig verifies that the inferenceservice ConfigMap
// exists (callers fail fast otherwise) and returns an empty config struct.
// ModelCache is loaded separately via NewModelCacheConfig.
func NewInferenceServicesConfig(clientset kubernetes.Interface) (*InferenceServicesConfig, error) {
	cm, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return inferenceServicesConfigFromConfigMap(cm)
}

// NewInferenceServicesConfigCached is the ConfigCache-backed variant of
// NewInferenceServicesConfig used on the reconcile hot path.
func NewInferenceServicesConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*InferenceServicesConfig, error) {
	cm, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return inferenceServicesConfigFromConfigMap(cm)
}

// inferenceServicesConfigFromConfigMap builds the InferenceServicesConfig from
// the ConfigMap. ModelCache is populated separately by the reconciler.
func inferenceServicesConfigFromConfigMap(cm *v1.ConfigMap) (*InferenceServicesConfig, error) {
	cfg := &InferenceServicesConfig{}
	if cm == nil {
		return cfg, nil
	}
	if data, ok := cm.Data[PodMonitorConfigName]; ok && strings.TrimSpace(data) != "" {
		if err := json.Unmarshal([]byte(data), &cfg.PodMonitor); err != nil {
			return nil, fmt.Errorf("unable to parse %q config json: %w", PodMonitorConfigName, err)
		}
	}
	if data, ok := cm.Data[AcceleratorResourcesConfigName]; ok && strings.TrimSpace(data) != "" {
		if err := json.Unmarshal([]byte(data), &cfg.AcceleratorResources); err != nil {
			return nil, fmt.Errorf("unable to parse %q config json: %w", AcceleratorResourcesConfigName, err)
		}
	}
	if data, ok := cm.Data[PodDisruptionBudgetConfigName]; ok && strings.TrimSpace(data) != "" {
		if err := validatePodDisruptionBudgetJSONKeys([]byte(data)); err != nil {
			return nil, fmt.Errorf("unable to parse %q config json: %w", PodDisruptionBudgetConfigName, err)
		}
		decoder := json.NewDecoder(strings.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg.PodDisruptionBudget); err != nil {
			return nil, fmt.Errorf("unable to parse %q config json: %w", PodDisruptionBudgetConfigName, err)
		}
		var trailing json.RawMessage
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				return nil, fmt.Errorf("unable to parse %q config json: multiple JSON values", PodDisruptionBudgetConfigName)
			}
			return nil, fmt.Errorf("unable to parse %q config json: %w", PodDisruptionBudgetConfigName, err)
		}
		policies := []struct {
			name   string
			policy *PodDisruptionBudgetPolicy
		}{
			{name: "rawDeployment", policy: cfg.PodDisruptionBudget.RawDeployment},
			{name: "omeNative", policy: cfg.PodDisruptionBudget.OMENative},
		}
		for _, mode := range policies {
			if mode.policy == nil {
				continue
			}
			path := PodDisruptionBudgetConfigName + "." + mode.name
			if (mode.policy.MinAvailable == nil) == (mode.policy.MaxUnavailable == nil) {
				return nil, fmt.Errorf("exactly one of %s.minAvailable or %s.maxUnavailable must be set", path, path)
			}
			if err := omevalidation.ValidatePodDisruptionBudget(
				path,
				mode.policy.MinAvailable,
				mode.policy.MaxUnavailable,
			); err != nil {
				return nil, err
			}
		}
	}
	return cfg, nil
}

func validatePodDisruptionBudgetJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil {
		return err
	}
	if start == nil {
		return nil
	}
	delim, ok := start.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("expected JSON object")
	}

	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return err
		}
		field, ok := fieldToken.(string)
		if !ok {
			return fmt.Errorf("expected JSON object field")
		}
		if _, duplicate := seen[field]; duplicate {
			return fmt.Errorf("json: duplicate field %q", field)
		}
		seen[field] = struct{}{}

		var policy json.RawMessage
		if err := decoder.Decode(&policy); err != nil {
			return err
		}
		switch field {
		case "rawDeployment", "omeNative":
			if err := validatePodDisruptionBudgetPolicyJSONKeys(policy); err != nil {
				return err
			}
		default:
			return fmt.Errorf("json: unknown field %q", field)
		}
	}
	_, err = decoder.Token()
	return err
}

func validatePodDisruptionBudgetPolicyJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil {
		return err
	}
	if start == nil {
		return nil
	}
	delim, ok := start.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("expected JSON object or null")
	}

	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return err
		}
		field, ok := fieldToken.(string)
		if !ok {
			return fmt.Errorf("expected JSON object field")
		}
		if _, duplicate := seen[field]; duplicate {
			return fmt.Errorf("json: duplicate field %q", field)
		}
		seen[field] = struct{}{}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		switch field {
		case "minAvailable", "maxUnavailable":
		default:
			return fmt.Errorf("json: unknown field %q", field)
		}
	}
	_, err = decoder.Token()
	return err
}

// NewOmeAgentConfig loads the ome-agent block from the inferenceservice
// ConfigMap. Returns a zero-value config (with no Image) if the block is
// absent so the BaseModel reconciler can surface a clearer error to the
// user via the model status when a PVC-backed model needs the Job.
func NewOmeAgentConfig(clientset kubernetes.Interface) (*OmeAgentConfig, error) {
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	cfg := &OmeAgentConfig{}
	if data, ok := configMap.Data[OmeAgentConfigName]; ok {
		if err := json.Unmarshal([]byte(data), cfg); err != nil {
			return nil, fmt.Errorf("unable to parse omeAgent config json: %w", err)
		}
	}
	return cfg, nil
}

func NewIngressConfig(clientset kubernetes.Interface) (*IngressConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseIngressConfig(configMap)
}

// NewIngressConfigCached is the ConfigCache-backed variant of NewIngressConfig
// used on the reconcile hot path.
func NewIngressConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*IngressConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseIngressConfig(configMap)
}

func parseIngressConfig(configMap *v1.ConfigMap) (*IngressConfig, error) {
	ingressConfig := &IngressConfig{}
	if ingress, ok := configMap.Data[IngressConfigKeyName]; ok {
		err := json.Unmarshal([]byte(ingress), &ingressConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to parse ingress config json: %w", err)
		}

		if ingressConfig.IngressGateway == "" || ingressConfig.IngressServiceName == "" {
			return nil, fmt.Errorf("invalid ingress config - ingressGateway and ingressService are required")
		}
		if ingressConfig.PathTemplate != "" {
			// TODO: ensure that the generated path is valid, that is:
			// * both Name and Namespace are used to avoid collisions
			// * starts with a /
			// For now simply check that this is a valid template.
			_, err := template.New("path-template").Parse(ingressConfig.PathTemplate)
			if err != nil {
				return nil, fmt.Errorf("invalid ingress config, unable to parse pathTemplate: %w", err)
			}
			if ingressConfig.IngressDomain == "" {
				return nil, fmt.Errorf("invalid ingress config - ingressDomain is required if pathTemplate is given")
			}
		}

		// An endpoint class is copied verbatim into status and
		// used as a selector / metrics / path token, so reject a malformed one at
		// config-load rather than emit an invalid status value later.
		if err := validateIngressGatewayClasses(ingressConfig); err != nil {
			return nil, err
		}
	}

	if ingressConfig.DomainTemplate == "" {
		ingressConfig.DomainTemplate = DefaultDomainTemplate
	}

	if ingressConfig.IngressDomain == "" {
		ingressConfig.IngressDomain = DefaultIngressDomain
	}

	if ingressConfig.UrlScheme == "" {
		ingressConfig.UrlScheme = DefaultUrlScheme
	}

	if len(ingressConfig.ConsistentHashHeaders) == 0 {
		ingressConfig.ConsistentHashHeaders = DefaultConsistentHashHeaders
	}

	return ingressConfig, nil
}

// validateIngressGatewayClasses rejects any endpoint class that is not a valid
// RFC-1123 DNS label. The class is copied verbatim onto status endpoints and is
// meant to be safe as a selector key / metrics dimension / path segment,
// so a malformed value is a config error, not something to
// sanitize silently. Empty classes are allowed (endpoint carries no class).
func validateIngressGatewayClasses(cfg *IngressConfig) error {
	if err := validateIngressClass("omeIngressGatewayClass", cfg.OmeIngressGatewayClass); err != nil {
		return err
	}
	for i := range cfg.AdditionalIngressGateways {
		field := fmt.Sprintf("additionalIngressGateways[%d].class", i)
		if err := validateIngressClass(field, cfg.AdditionalIngressGateways[i].Class); err != nil {
			return err
		}
	}
	for ns, ng := range cfg.NamespaceIngressGateways {
		field := fmt.Sprintf("namespaceIngressGateways[%q].primary.class", ns)
		if err := validateIngressClass(field, ng.Primary.Class); err != nil {
			return err
		}
		for i := range ng.Additional {
			field := fmt.Sprintf("namespaceIngressGateways[%q].additional[%d].class", ns, i)
			if err := validateIngressClass(field, ng.Additional[i].Class); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateIngressClass(field, class string) error {
	if class == "" {
		return nil
	}
	if errs := validation.IsDNS1123Label(class); len(errs) > 0 {
		return fmt.Errorf("invalid ingress config - %s %q must be an RFC-1123 label: %s",
			field, class, strings.Join(errs, "; "))
	}
	return nil
}

func getComponentConfig(key string, configMap *v1.ConfigMap, componentConfig interface{}) error {
	if data, ok := configMap.Data[key]; ok {
		err := json.Unmarshal([]byte(data), componentConfig)
		if err != nil {
			return fmt.Errorf("unable to unmarshall %v json string due to %w ", key, err)
		}
	}
	return nil
}

func NewDeployConfig(clientset kubernetes.Interface) (*DeployConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseDeployConfig(configMap)
}

// NewDeployConfigCached is the ConfigCache-backed variant of NewDeployConfig
// used on the reconcile hot path.
func NewDeployConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*DeployConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseDeployConfig(configMap)
}

func parseDeployConfig(configMap *v1.ConfigMap) (*DeployConfig, error) {
	deployConfig := &DeployConfig{}
	if deploy, ok := configMap.Data[DeployConfigName]; ok {
		err := json.Unmarshal([]byte(deploy), &deployConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to parse deploy config json: %w", err)
		}

		if deployConfig.DefaultDeploymentMode == "" {
			return nil, fmt.Errorf("invalid deploy config, defaultDeploymentMode is required")
		}

		if deployConfig.DefaultDeploymentMode != string(constants.RawDeployment) {
			return nil, fmt.Errorf("invalid deployment mode. Supported default mode is %s", constants.RawDeployment)
		}

		if deployConfig.Replicas != nil {
			if err := deployConfig.Replicas.validate(); err != nil {
				return nil, err
			}
		}

		if v := deployConfig.TerminationGracePeriodSeconds; v != nil && *v <= 0 {
			return nil, fmt.Errorf("invalid deploy config, terminationGracePeriodSeconds must be > 0, got %d", *v)
		}

		if v := deployConfig.MinReadySeconds; v != nil && *v < 0 {
			return nil, fmt.Errorf("invalid deploy config, minReadySeconds must be >= 0, got %d", *v)
		}

		if err := deployConfig.UpdateStrategy.validate(); err != nil {
			return nil, err
		}
	}
	return deployConfig, nil
}

// NewCanaryAnalysisConfig loads the canary metric-analysis defaults from the
// inferenceservice-config ConfigMap, applying operational fallbacks for the
// sampler tunables. BundledPrometheusAddress has no fallback (the chart is the
// source of truth); empty leaves canaries without a default metrics source.
func NewCanaryAnalysisConfig(clientset kubernetes.Interface) (*CanaryAnalysisConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseCanaryAnalysisConfig(configMap)
}

// NewCanaryAnalysisConfigCached is the ConfigCache-backed variant of
// NewCanaryAnalysisConfig used on the reconcile hot path.
func NewCanaryAnalysisConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*CanaryAnalysisConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseCanaryAnalysisConfig(configMap)
}

func parseCanaryAnalysisConfig(configMap *v1.ConfigMap) (*CanaryAnalysisConfig, error) {
	cfg := &CanaryAnalysisConfig{}
	if err := getComponentConfig(CanaryAnalysisConfigName, configMap, cfg); err != nil {
		return nil, fmt.Errorf("unable to parse canaryAnalysis config json: %w", err)
	}
	if cfg.QueryTimeout == "" {
		cfg.QueryTimeout = DefaultAnalysisQueryTimeout
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = DefaultAnalysisMaxConcurrency
	}
	if cfg.CacheTTL == "" {
		cfg.CacheTTL = DefaultAnalysisCacheTTL
	}
	return cfg, nil
}

// NewCoordinationConfig loads the OMENative coordination tuning from the
// inferenceservice-config ConfigMap. There are no in-code defaults: an absent
// "coordination" key yields a zero-valued config (deadband disabled), preserving
// the prior write-on-any-diff behavior.
func NewCoordinationConfig(clientset kubernetes.Interface) (*CoordinationConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseCoordinationConfig(configMap)
}

// NewCoordinationConfigCached is the ConfigCache-backed variant of
// NewCoordinationConfig used on the reconcile hot path.
func NewCoordinationConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*CoordinationConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseCoordinationConfig(configMap)
}

func parseCoordinationConfig(configMap *v1.ConfigMap) (*CoordinationConfig, error) {
	cfg := &CoordinationConfig{}
	if err := getComponentConfig(CoordinationConfigName, configMap, cfg); err != nil {
		return nil, fmt.Errorf("unable to parse coordination config json: %w", err)
	}
	return cfg, nil
}

// QueryTimeoutDuration parses QueryTimeout, falling back to the known-good
// DefaultAnalysisQueryTimeout when an operator wrote an unparsable or
// non-positive value. The timeout MUST be positive: a zero/negative value would
// leave a background analysis query unbounded, so a hung Prometheus could park a
// sampler goroutine indefinitely.
func (c *CanaryAnalysisConfig) QueryTimeoutDuration() time.Duration {
	return parseDurationOr(c.QueryTimeout, DefaultAnalysisQueryTimeout)
}

// CacheTTLDuration parses CacheTTL, falling back to the known-good
// DefaultAnalysisCacheTTL when an operator wrote an unparsable or non-positive
// value. The TTL MUST be positive: a zero/negative value disables cache eviction
// (unbounded growth).
func (c *CanaryAnalysisConfig) CacheTTLDuration() time.Duration {
	return parseDurationOr(c.CacheTTL, DefaultAnalysisCacheTTL)
}

// parseDurationOr parses s, returning the parse of fallback (a compile-time
// constant known to parse) when s is empty, malformed, or non-positive — callers
// rely on a strictly positive result.
func parseDurationOr(s, fallback string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	d, _ := time.ParseDuration(fallback)
	return d
}

// BenchmarkJobConfigName is the inferenceservice-config key holding the
// benchmark-job image/resource block.
const BenchmarkJobConfigName = "benchmarkjob"

type BenchmarkJobConfig struct {
	// PodConfig contains all Pod Configuration
	PodConfig PodConfig `json:"podConfig"`
}

func NewBenchmarkJobConfig(clientset kubernetes.Interface) (*BenchmarkJobConfig, error) {
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(context.TODO(), constants.BenchmarkJobConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	benchmarkJobConfig := &BenchmarkJobConfig{}
	for _, err := range []error{
		getComponentConfig(BenchmarkJobConfigName, configMap, &benchmarkJobConfig),
	} {
		if err != nil {
			return nil, err
		}
	}
	return benchmarkJobConfig, nil
}

// getInferenceServiceConfigMap fetches the inferenceservice-config ConfigMap
// from the OME namespace.

type PodConfig struct {
	Image         string `json:"image"`
	CPURequest    string `json:"cpuRequest"`
	MemoryRequest string `json:"memoryRequest"`
	CPULimit      string `json:"cpuLimit"`
	MemoryLimit   string `json:"memoryLimit"`
}

// +kubebuilder:object:generate=false
