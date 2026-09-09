package inferenceservice

import (
	"context"
	"fmt"
	"strings"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	policyv1 "k8s.io/api/policy/v1"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/components"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/external_service"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	multimodelconfig "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/modelconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	traffic "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	isvcstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	rolloutpolicycontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils"
)

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices;inferenceservices/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes;servingruntimes/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=servingruntimes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes;clusterservingruntimes/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterservingruntimes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels;basemodels/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=finetunedweights/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=finetunedweights;finetunedweights/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels;basemodels/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networking.istio.io,resources=sidecars,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.istio.io,resources=destinationrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=update;patch
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=keda.sh,resources=triggerauthentications,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=ome.io,resources=autoscalerpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=rolloutpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scheduling.x-k8s.io,resources=podgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gateway.envoyproxy.io,resources=backendtrafficpolicies,verbs=get;list;watch;create;update;patch;delete

// InferenceServiceState describes the Readiness of the InferenceService
type InferenceServiceState string

// Different InferenceServiceState an InferenceService may have.
const (
	InferenceServiceReadyState    InferenceServiceState = "InferenceServiceReady"
	InferenceServiceNotReadyState InferenceServiceState = "InferenceServiceNotReady"
)

const inferenceServiceFinalizer = "inferenceservice.finalizers"

// InferenceServiceReconciler reconciles an InferenceService object
type InferenceServiceReconciler struct {
	client.Client
	ClientConfig *rest.Config
	Clientset    kubernetes.Interface
	// APIReader is the AuthoritativeReader role (see type docs).
	// Populated from mgr.GetAPIReader() in SetupWithManager. Threaded through the
	// component builder to OMENative for reads where cache lag would
	// be a correctness problem (revision bookkeeping, audit ledger,
	// EndpointSlice drain checks).
	APIReader client.Reader
	// Expectations is the OMENative create/delete bookkeeping cache.
	// SetupWithManager initializes it (if nil) and registers the Pod
	// event handler against the same instance the component dispatch
	// path threads into ReconcileParams.
	Expectations    *omenative.Expectations
	Log             logr.Logger
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	RuntimeSelector runtimeselector.Selector
	// AcceleratorClassSelector resolves the AcceleratorClass for each
	// Engine/Decoder component (explicit name, or policy over the
	// runtime's acceleratorRequirements). Initialized in SetupWithManager.
	AcceleratorClassSelector acceleratorclassselector.Selector
	// TrafficReconciler is the backend-policy reconciler.
	// Built from the active translator at controller startup; the
	// active translator is selected by traffic/factory.New based on
	// installed Gateway-implementation CRDs.
	TrafficReconciler *traffic.Reconciler
	// GangSchedulingAvailable is the cluster-discovery boolean — true
	// when the scheduler-plugins `scheduling.x-k8s.io/v1alpha1/PodGroup`
	// CRD is installed at controller startup. Threaded through the
	// component builder factory to OMENative's reconciler so the gang-
	// scheduling code path runs only when the CRD is actually present.
	// When false, multi-pod OMENative Instances still create pods but
	// the controller stamps a `GangSchedulingUnavailable=True`
	// Component condition.
	GangSchedulingAvailable bool
	// KedaAvailable is the cluster-discovery boolean for the KEDA CRDs,
	// probed once in SetupWithManager. The policy layer consults it so a
	// rendered class=KEDA block on a KEDA-less cluster fails closed
	// (ClassUnavailable) instead of erroring dispatch on every pass.
	KedaAvailable bool
	// RolloutPolicyEnabled is the cluster-discovery boolean for the
	// RolloutPolicy CRD (chart-gated with its webhook + status controller).
	// The run layer pins plans regardless; only ref RESOLUTION needs the
	// feature — with it absent, a stored ref parks the rollout instead of
	// silently no-oping.
	RolloutPolicyEnabled bool
	// AutoscalerPolicyEnabled is the cluster-discovery boolean for the
	// AutoscalerPolicy CRD. The chart installs CRD + controller wiring +
	// webhook together; when the CRD is absent no watch or index is
	// registered and any spec.<component>.autoscalerPolicyRef fails closed
	// at reconcile (admission additionally rejects refs on such clusters).
	AutoscalerPolicyEnabled bool
	// CanarySampler runs canary analysis Prometheus queries off the reconcile
	// goroutine: reconcile reads cached results non-blocking, and a completion
	// event re-reconciles the ISVC. Built once in SetupWithManager from the
	// canaryAnalysis operator config; nil disables metric-gated steps (they hold).
	CanarySampler *canary.Sampler
	// MaxConcurrentReconciles caps parallel ISVC reconciles (distinct objects
	// only — controller-runtime serializes per object key, so independent ISVCs
	// reconcile in parallel safely). Sourced from a flag/chart value; no in-code
	// default. Zero (unset) preserves controller-runtime's single-worker default.
	MaxConcurrentReconciles int
	// ConfigCache memoizes the inferenceservice-config ConfigMap for a short,
	// flag-driven TTL so a single reconcile pass shares one apiserver GET across
	// the Deploy / InferenceServices / Ingress / CanaryAnalysis config loads
	// instead of issuing one uncached Clientset GET each. A nil cache (or a
	// non-positive TTL) falls through to the apiserver every call, preserving the
	// pre-cache behavior; the short TTL keeps "ConfigMap edits apply without a
	// restart" intact. Initialized in SetupWithManager.
	ConfigCache *controllerconfig.ConfigCache
	// ConfigCacheTTL is the TTL applied to ConfigCache. Flag-driven (no in-code
	// behavioral default — supplied by the manager's --config-cache-ttl flag /
	// chart value); a non-positive value disables caching (always reads the
	// apiserver).
	ConfigCacheTTL time.Duration
	// componentDeps is the process-lifetime ComponentDeps prototype,
	// built once in SetupWithManager. buildComponentDeps attaches the
	// per-reconcile config; every other field is immutable after setup.
	componentDeps *components.ComponentDeps
}

func (r *InferenceServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch the InferenceService instance
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, req.NamespacedName, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	// Bind structured logging context for this reconcile pass: every
	// per-step log line and every downstream callback that pulls from
	// ctx inherits (namespace, isvc) without re-stamping per-line. The
	// existing r.Log field is preserved for sites that already key off
	// the receiver — those use the same backing logger so output stays
	// consistent.
	log := r.Log.WithValues("namespace", isvc.Namespace, "isvc", isvc.Name)
	ctx = ctrl.LoggerInto(ctx, log)
	// get annotations from isvc
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	deployConfig, err := controllerconfig.NewDeployConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create DeployConfig")
	}

	// For backward compatibility with predictor-based architecture
	deploymentMode := isvcutils.GetDeploymentMode(annotations, deployConfig)
	log.V(1).Info("InferenceService deployment mode resolved", "deploymentMode", deploymentMode)

	// examine DeletionTimestamp to determine if object is under deletion
	if isvc.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object. This is equivalent
		// registering our finalizer.
		if !controllerutil.ContainsFinalizer(isvc, inferenceServiceFinalizer) {
			controllerutil.AddFinalizer(isvc, inferenceServiceFinalizer)
			if err := r.Update(ctx, isvc); err != nil {
				// A conflict means another writer advanced the object after
				// this reconcile read it. Requeue so the next pass applies the
				// finalizer to a fresh object.
				if apierrors.IsNotFound(err) {
					return ctrl.Result{}, nil
				}
				if apierrors.IsConflict(err) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, err
			}
		}
	} else {
		// The object is being deleted
		if controllerutil.ContainsFinalizer(isvc, inferenceServiceFinalizer) {
			// Drop this ISVC's OMENative coordination, canary, and readiness
			// metric series so the per-(namespace,isvc) vectors do not leak
			// unbounded after teardown. Idempotent and runs on the
			// finalizer-bearing delete pass.
			coordination.DeleteForISVC(isvc.Namespace, isvc.Name)
			canary.DeleteForISVC(isvc.Namespace, isvc.Name)
			obsmetrics.DeleteISVCSeries(isvc.Namespace, isvc.Name)

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(isvc, inferenceServiceFinalizer)
			if err := r.Update(ctx, isvc); err != nil {
				if apierrors.IsNotFound(err) {
					return ctrl.Result{}, nil
				}
				if apierrors.IsConflict(err) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, err
			}
		}

		// Stop reconciliation as the item is being deleted
		return ctrl.Result{}, nil
	}

	// Handle VirtualDeployment without actual reconciliation
	if deploymentMode == constants.VirtualDeployment {
		return r.handleVirtualDeployment(isvc)
	}

	// Initialize status if not already initialized
	if isvc.Status.Components == nil {
		isvc.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
	}

	// Setup reconcilers.
	// apiVersion is omitted: controller-runtime strips TypeMeta from
	// objects fetched via the typed cache, so isvc.APIVersion is always
	// empty here. Use v1beta1.SchemeGroupVersion.String() if a callable
	// API version is needed downstream.
	log.V(1).Info("Reconciling inference service")
	isvcConfig, err := controllerconfig.NewInferenceServicesConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create InferenceServicesConfig")
	}

	modelConfigReconciler := multimodelconfig.NewModelConfigReconciler(r.Client, r.Clientset, r.Scheme)
	result, err := modelConfigReconciler.Reconcile(ctx, isvc) // Added ctx
	if err != nil {
		return result, err
	}

	cdeps := r.buildComponentDeps(isvcConfig)

	// The policy resolver is rebuilt per reconcile from the cached operator
	// config so provider-binding edits apply within one config TTL. With the
	// feature absent, Enabled=false fails any ref closed instead of silently
	// ignoring it.
	policyResolver := &autoscaler.PolicyResolver{Enabled: false}
	if r.AutoscalerPolicyEnabled {
		policyConfig, err := controllerconfig.NewAutoscalerPolicyConfigCached(r.ConfigCache, r.Clientset)
		if err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "fails to load autoscalerPolicy config")
		}
		providers, providerAuth := autoscaler.ProviderBindingsFromConfig(policyConfig)
		policyResolver = &autoscaler.PolicyResolver{
			Client:        r.Client,
			Providers:     providers,
			ProviderAuth:  providerAuth,
			Enabled:       true,
			KedaAvailable: r.KedaAvailable,
			// Holds caused by operator-config state (unbound provider) heal
			// on config edits, which emit no ISVC event — requeue once per
			// config cache TTL so consumers recover unattended. TTL<=0 means
			// live config reads; no periodic requeue is wired then.
			HoldRequeueAfter: r.ConfigCacheTTL,
		}
	}

	// Determine which components to reconcile based on the spec
	var reconcilers []components.Component

	var ingressDeploymentMode constants.DeploymentModeType

	// Step 1: Reconcile model first
	baseModel, baseModelMeta, baseModelStatus, err := isvcutils.ReconcileBaseModelWithStatus(r.Client, isvc)
	if err != nil {
		r.Log.Error(err, "Failed to reconcile base model", "Name", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "ModelReconcileError", err.Error())
		return reconcile.Result{}, err
	}
	// Lean path: no spec.model. Operator must specify spec.runtime
	// directly; we skip model fetch, sharded-readiness gating, overlay
	// resolution, and runtime-vs-model validation.
	if baseModel != nil && isvcutils.IsShardedBaseModel(baseModel) {
		ready, message := isvcutils.ShardedBaseModelReady(baseModelStatus, baseModelMeta.Generation)
		if !ready {
			if message == "" {
				message = "sharded BaseModel is not ready"
			}
			r.Log.Info("Waiting for sharded BaseModel to become ready",
				"model", isvc.Spec.Model.Name,
				"namespace", isvc.Namespace,
				"message", message)
			r.Recorder.Event(isvc, v1.EventTypeNormal, "ModelNotReady", message)
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// Resolve overlays. Failures here (NotFound, disabled, sharded
	// NotReady) become Skipped entries — only transient client errors
	// abort the reconcile.
	resolvedOverlays, err := isvcutils.ResolveOverlays(r.Client, isvc)
	if err != nil {
		r.Log.Error(err, "Failed to resolve overlays", "isvc", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "OverlayResolveError", err.Error())
		return reconcile.Result{}, err
	}
	isvc.Status.MountedOverlays = components.MountedOverlaySummary(resolvedOverlays)
	setOverlaysReadyCondition(isvc, resolvedOverlays)

	// Step 2: Get runtime spec.
	//   - runtime explicit  → fetch by name; validate against model only
	//     if model is present.
	//   - runtime omitted  → auto-select from model; requires a model.
	var rt *v1beta1.ServingRuntimeSpec
	var rtName string
	var rtIsCluster bool
	userSpecifiedRuntime := false

	switch {
	case isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "":
		rtName = isvc.Spec.Runtime.Name
		userSpecifiedRuntime = true

		// When the user opts into pinning (autoSync=false), fetch the
		// pinned ControllerRevision rather than the live runtime. The
		// pin helper handles first-reconcile create, drift detection,
		// and ome.io/runtime-sync ack — and persists status itself when
		// children must be skipped.
		if isvc.Spec.Runtime.AutoSync != nil && !*isvc.Spec.Runtime.AutoSync {
			pin, perr := r.resolvePinnedRuntime(ctx, isvc)
			if perr != nil {
				r.Log.Error(perr, "Pin resolution failed", "runtime", rtName)
				r.Recorder.Event(isvc, v1.EventTypeWarning, "RuntimePinError", perr.Error())
				return reconcile.Result{}, perr
			}
			if pin.skipChildren {
				return reconcile.Result{}, nil
			}
			rt = pin.spec
			rtIsCluster = sourceKindFor(isvc.Spec.Runtime) == runtimerevision.KindClusterServingRuntime
			break
		}

		if baseModel != nil {
			if err := r.RuntimeSelector.ValidateRuntime(ctx, rtName, baseModel, isvc); err != nil {
				// The operator named this runtime explicitly, so OME should not
				// block on the runtime's *declared* supportedModelFormats: a
				// generic runtime (e.g. sglang) can serve many architectures it
				// never enumerates. Downgrade a pure compatibility mismatch
				// (format / architecture / framework) to an advisory event and
				// proceed — the deliberate choice wins over the declaration.
				//
				// A named-but-missing runtime gets the same permanent-config
				// parking treatment as the fetch below: advisory condition +
				// event, no error requeue, self-heals via the runtime watch.
				//
				// Everything else stays a hard error: a disabled runtime or a
				// malformed model genuinely cannot run, and a sharded model
				// with no configured cache provider physically cannot load
				// (the webhook's modelRequiresCacheProvider guard is the same
				// sharded check) — keep gating those.
				switch {
				case runtimeselector.IsRuntimeCompatibilityError(err) && !isvcutils.IsShardedBaseModel(baseModel):
					r.Log.Info("Runtime named explicitly; proceeding despite declared-format mismatch",
						"runtime", rtName, "model", isvc.Spec.Model.Name, "details", err.Error())
					r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeCompatibilityAdvisory",
						"Runtime %s does not declare support for model %s (%v); proceeding because the runtime was named explicitly",
						rtName, isvc.Spec.Model.Name, err)
				case runtimeselector.IsRuntimeNotFoundError(err):
					return r.markRuntimeUnresolved(isvc, deploymentMode, rtName, err)
				default:
					r.Log.Error(err, "Runtime validation failed", "runtime", rtName, "model", isvc.Spec.Model.Name)
					r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeValidationError",
						"Runtime %s does not support model %s: %v", rtName, isvc.Spec.Model.Name, err)
					return reconcile.Result{}, err
				}
			}
		}
		rtSpec, isCluster, err := r.RuntimeSelector.GetRuntime(ctx, rtName, isvc.Namespace, runtimeselector.RefKind(isvc.Spec.Runtime))
		if err != nil {
			if runtimeselector.IsRuntimeNotFoundError(err) {
				// A named-but-missing runtime is a permanent user-config error,
				// not a transient failure. Don't error+requeue (which hot-loops
				// and spams ERROR logs) and don't touch a currently-serving
				// ISVC — surface it as an advisory condition + event and wait
				// for the ServingRuntime watch to re-trigger once the runtime
				// exists.
				return r.markRuntimeUnresolved(isvc, deploymentMode, rtName, err)
			}
			r.Log.Error(err, "Failed to get runtime spec", "runtime", rtName)
			r.Recorder.Event(isvc, v1.EventTypeWarning, "RuntimeFetchError", err.Error())
			return reconcile.Result{}, err
		}
		r.clearRuntimeUnresolved(isvc)
		rt = rtSpec
		rtIsCluster = isCluster
	case baseModel != nil:
		selection, err := r.RuntimeSelector.SelectRuntime(ctx, baseModel, isvc)
		if err != nil {
			if runtimeselector.IsNoRuntimeFoundError(err) || runtimeselector.IsRuntimeNotFoundError(err) {
				// No compatible runtime exists for the model yet — same
				// permanent-config, non-destructive treatment as the explicit
				// path: advisory condition + event, no error requeue, self-heals
				// when a matching runtime is created (watch re-triggers).
				return r.markRuntimeUnresolved(isvc, deploymentMode, isvc.Spec.Model.Name, err)
			}
			r.Log.Error(err, "Failed to auto-select runtime", "model", isvc.Spec.Model.Name)
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeSelectionError",
				"Failed to find runtime for model %s: %v", isvc.Spec.Model.Name, err)
			return reconcile.Result{}, err
		}
		r.clearRuntimeUnresolved(isvc)
		rt = selection.Spec
		rtName = selection.Name
		rtIsCluster = selection.IsCluster
		log.Info("Auto-selected runtime", "runtime", rtName, "model", isvc.Spec.Model.Name)
	default:
		// No model, no runtime — webhook should have caught this; defensive
		// guard for direct client writes that bypass admission.
		err := fmt.Errorf("InferenceService must specify spec.runtime when spec.model is omitted")
		r.Log.Error(err, "Cannot reconcile", "Name", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "RuntimeSelectionError", err.Error())
		return reconcile.Result{}, err
	}

	if err := validateResolvedRuntimeEnabled(rt, rtName, rtIsCluster); err != nil {
		r.Log.Error(err, "Runtime validation failed", "runtime", rtName)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeValidationError",
			"Runtime %s failed validation: %v", rtName, err)
		return reconcile.Result{}, err
	}

	// Step 3: Merge rt and isvc specs to get final engine, decoder, and router specs
	mergedEngine, mergedDecoder, mergedRouter, err := isvcutils.MergeRuntimeSpecs(isvc, rt, r.Log)
	if err != nil {
		r.Log.Error(err, "Failed to merge specs", "Name", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "MergeSpecsError", err.Error())
		return reconcile.Result{}, err
	}

	// The effective serving container ports are the resolved view of the
	// port each Component exposes. OMENative's per-revision Service
	// producers (coordination, canary) publish their routing Service on
	// this port, so it must be captured here and threaded down.
	componentRunnerPorts := isvcutils.MergedRunnerPorts(mergedEngine, mergedDecoder, mergedRouter)

	// Rollout run layer: resolve and pin the effective plan BEFORE the
	// partition stamp and the dispatch engines, so every consumer in this
	// pass indexes the same frozen plan. Pinning is not chart-gated; only
	// ref resolution depends on the policy feature. The pin and the executors'
	// step state ride the same status flush, which is what makes them
	// atomic.
	var rolloutRunRequeue time.Duration
	var rolloutConfig *controllerconfig.RolloutConfig
	var metricProviders controllerconfig.MetricProvidersConfig
	if isvc.Spec.Rollout != nil || isvc.Status.Rollout != nil {
		rolloutConfig, err = controllerconfig.NewRolloutConfigCached(r.ConfigCache, r.Clientset)
		if err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "fails to load rollout config")
		}
		metricProviders, err = controllerconfig.NewMetricProvidersConfigCached(r.ConfigCache, r.Clientset)
		if err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "fails to load metricProviders config")
		}
		bound := make(map[string]struct{}, len(metricProviders))
		for name := range metricProviders {
			bound[name] = struct{}{}
		}
		runOutcome, runErr := rolloutrun.Reconcile(ctx, rolloutrun.Inputs{
			Client:         r.Client,
			Reader:         r.APIReader,
			Recorder:       r.Recorder,
			ISVC:           isvc,
			FeatureEnabled: r.RolloutPolicyEnabled,
			BoundProviders: bound,
		})
		if runErr != nil {
			log.Error(runErr, "Failed to reconcile rollout run")
			return reconcile.Result{}, errors.Wrapf(runErr, "fails to reconcile rollout run")
		}
		rolloutRunRequeue = runOutcome.RequeueAfter
		// A run boundary (open/close/repin) is persisted IMMEDIATELY: the pin
		// is load-bearing for the update gates and the partition stamp, and a
		// fallible reconciler erroring downstream would otherwise drop the
		// in-memory pin — reopening the run next pass and flapping the
		// projected spec under the executors.
		if runOutcome.StateChanged {
			if err := r.updateStatus(isvc, deploymentMode); err != nil {
				r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
				return reconcile.Result{}, err
			}
		}
	}

	// Canary: when the effective plan carries a canary group, stamp the
	// current step's RollingUpdate.Partition onto each merged Component
	// before the Component reconcilers run, so the standard
	// spec→IR→plan→HeldByPartition path holds the staged old/new split. The
	// step machine + traffic run in Step 6a below.
	if v1beta1.EffectiveCanaryGroup(isvc) != nil {
		if mergedEngine != nil {
			canary.StampStepPartition(isvc, v1beta1.EngineComponent, &mergedEngine.ComponentExtensionSpec)
		}
		if mergedDecoder != nil {
			canary.StampStepPartition(isvc, v1beta1.DecoderComponent, &mergedDecoder.ComponentExtensionSpec)
		}
		if mergedRouter != nil {
			canary.StampStepPartition(isvc, v1beta1.RouterComponent, &mergedRouter.ComponentExtensionSpec)
		}
	} else {
		// A canary-KIND group with no resolvable plan (ref-only pre-open, or
		// parked) stamps a full hold instead of no partition, keeping the
		// projected spec deterministic across run states.
		if mergedEngine != nil {
			canary.StampPlanGateHold(isvc, v1beta1.EngineComponent, &mergedEngine.ComponentExtensionSpec)
		}
		if mergedDecoder != nil {
			canary.StampPlanGateHold(isvc, v1beta1.DecoderComponent, &mergedDecoder.ComponentExtensionSpec)
		}
		if mergedRouter != nil {
			canary.StampPlanGateHold(isvc, v1beta1.RouterComponent, &mergedRouter.ComponentExtensionSpec)
		}
	}

	// Step 4: Determine deployment modes based on merged specs
	engineDeploymentMode, decoderDeploymentMode, routerDeploymentMode, err := isvcutils.DetermineDeploymentModes(mergedEngine, mergedDecoder, mergedRouter, rt, isvc.Spec.DeploymentMode)
	if err != nil {
		r.Log.Error(err, "Failed to determine deployment modes", "Name", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "DeploymentModeError", err.Error())
		return reconcile.Result{}, err
	}
	componentDeploymentModes := make(map[v1beta1.ComponentType]constants.DeploymentModeType, 3)
	if mergedEngine != nil {
		componentDeploymentModes[v1beta1.EngineComponent] = engineDeploymentMode
	}
	if mergedDecoder != nil {
		componentDeploymentModes[v1beta1.DecoderComponent] = decoderDeploymentMode
	}
	if mergedRouter != nil {
		componentDeploymentModes[v1beta1.RouterComponent] = routerDeploymentMode
	}

	// If both engine and decoder exist, it's PD-disaggregated. V(1):
	// steady-state per-reconcile breadcrumb; no operator action.
	if mergedEngine != nil && mergedDecoder != nil {
		log.V(1).Info("PD-disaggregated deployment detected")
	}

	// Step 5: Create reconcilers based on merged specs. The
	// "Creating <component> reconciler" / "Determined ingress
	// deployment mode" lines fire every reconcile but observe no
	// state change — demoted to V(1) because they're hot-path /
	// steady-state breadcrumbs.
	if mergedEngine != nil {
		engineACObj, engineAcName, err := r.AcceleratorClassSelector.GetAcceleratorClass(ctx, isvc, rt, v1beta1.EngineComponent)
		if err != nil {
			log.Error(err, "Failed to get accelerator class for engine component")
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "AcceleratorClassError", "Failed to get accelerator class for engine: %v", err)
			return reconcile.Result{}, err
		}
		var engineAC *v1beta1.AcceleratorClassSpec
		if engineACObj != nil {
			engineAC = &engineACObj.Spec
		}
		engineSupportedModelFormats := r.RuntimeSelector.GetSupportedModelFormat(ctx, rt, baseModel, userSpecifiedRuntime)
		log.V(1).Info("Creating engine reconciler",
			"deploymentMode", engineDeploymentMode,
			"acceleratorClass", engineAcName)

		engineReconciler := components.NewEngine(cdeps, components.ComponentInputs{
			DeploymentMode:       engineDeploymentMode,
			BaseModel:            baseModel,
			BaseModelMeta:        baseModelMeta,
			Runtime:              rt,
			RuntimeName:          rtName,
			ModelFormat:          engineSupportedModelFormats,
			AcceleratorClass:     engineAC,
			AcceleratorClassName: engineAcName,
			Overlays:             resolvedOverlays,
			PolicyResolver:       policyResolver,
		}, mergedEngine)
		reconcilers = append(reconcilers, engineReconciler)
	}

	if mergedDecoder != nil {
		decoderACObj, decoderAcName, err := r.AcceleratorClassSelector.GetAcceleratorClass(ctx, isvc, rt, v1beta1.DecoderComponent)
		if err != nil {
			log.Error(err, "Failed to get accelerator class for decoder component")
			r.Recorder.Eventf(isvc, v1.EventTypeWarning, "AcceleratorClassError", "Failed to get accelerator class for decoder: %v", err)
			return reconcile.Result{}, err
		}
		var decoderAC *v1beta1.AcceleratorClassSpec
		if decoderACObj != nil {
			decoderAC = &decoderACObj.Spec
		}
		decoderSupportedModelFormats := r.RuntimeSelector.GetSupportedModelFormat(ctx, rt, baseModel, userSpecifiedRuntime)
		log.V(1).Info("Creating decoder reconciler",
			"deploymentMode", decoderDeploymentMode,
			"acceleratorClass", decoderAcName)

		decoderReconciler := components.NewDecoder(cdeps, components.ComponentInputs{
			DeploymentMode:       decoderDeploymentMode,
			BaseModel:            baseModel,
			BaseModelMeta:        baseModelMeta,
			Runtime:              rt,
			RuntimeName:          rtName,
			ModelFormat:          decoderSupportedModelFormats,
			AcceleratorClass:     decoderAC,
			AcceleratorClassName: decoderAcName,
			Overlays:             resolvedOverlays,
			PolicyResolver:       policyResolver,
		}, mergedDecoder)
		reconcilers = append(reconcilers, decoderReconciler)
	}

	// Add Router reconciler if merged router spec exists (using new v2 Router)
	if mergedRouter != nil {
		log.V(1).Info("Creating router reconciler",
			"deploymentMode", routerDeploymentMode)

		// Router has no supported-model-format input — ModelFormat stays nil.
		routerReconciler := components.NewRouter(cdeps, components.ComponentInputs{
			DeploymentMode: routerDeploymentMode,
			BaseModel:      baseModel,
			BaseModelMeta:  baseModelMeta,
			Runtime:        rt,
			RuntimeName:    rtName,
			Overlays:       resolvedOverlays,
			PolicyResolver: policyResolver,
		}, mergedRouter) // merged router spec, not isvc.Spec.Router
		reconcilers = append(reconcilers, routerReconciler)
	}

	// Determine the correct ingress deployment mode using the same logic as ingress reconciler
	// but with the already-determined deployment modes to avoid inconsistency
	if mergedRouter != nil {
		ingressDeploymentMode = routerDeploymentMode
	} else if mergedDecoder != nil {
		ingressDeploymentMode = decoderDeploymentMode
	} else {
		ingressDeploymentMode = engineDeploymentMode
	}

	log.V(1).Info("Determined ingress deployment mode",
		"ingressDeploymentMode", ingressDeploymentMode)

	// Step 6: Run all reconcilers. Capture the max-requeue across
	// per-Component reconcilers WITHOUT short-circuiting — coordination
	// must run at the end of every reconcile pass even when individual
	// Components requeue (which they do constantly during active
	// rollouts). A loop that returned on the first requeuing
	// reconciler would leave Status.RolloutCoordination /
	// per-revision Services / Traffic[] stale until convergence.
	var pendingRequeue reconcile.Result
	for _, reconciler := range reconcilers {
		result, err := reconciler.Reconcile(ctx, isvc)
		if err != nil {
			log.Error(err, "Failed to reconcile component",
				"component", fmt.Sprintf("%T", reconciler))
			return result, err
		}
		if result.Requeue {
			pendingRequeue.Requeue = true
		}
		if result.RequeueAfter > pendingRequeue.RequeueAfter {
			pendingRequeue.RequeueAfter = result.RequeueAfter
		}
	}

	// Step 6a: Run cross-Component coordination once every Component
	// reconciler has finished. The coordination layer reads observed
	// per-revision pod counts, ensures per-revision Services, writes
	// Status.Components.<c>.Traffic[], drives the per-group state
	// machine, and stamps RolloutCoordinationReady. Independent of
	// any single Component reconciler so the ingress / traffic
	// reconciliation downstream reads the freshest producer-side
	// status.
	// Rollout dispatch (v2 groups): a canary group drives the canary step machine;
	// blueGreen/rollingUpdate groups drive the coordination engine. v2 groups
	// partition the Components into disjoint sets, so both engines may run in one
	// pass — each no-ops when its group type is absent (canary.Dispatch on no
	// canary group; coordination.Reconcile on no coordination-style group). Both
	// observe per-revision pods, ensure per-revision Services, and write
	// Status.Components.<c>.Traffic + phase for their own Components. Cross-group
	// ordering (the group sequencer) is a follow-on; today the groups run
	// concurrently on their disjoint Components.
	if rolloutRunRequeue > pendingRequeue.RequeueAfter {
		pendingRequeue.RequeueAfter = rolloutRunRequeue
	}
	if v1beta1.EffectiveCanaryGroup(isvc) != nil {
		// Resolve the metrics source + query timeout per-reconcile from the
		// canaryAnalysis operator config (so ConfigMap edits take effect without a
		// restart); the sampler's structural tuning is fixed at startup. Only
		// analysis steps read these — non-analysis canaries ignore them.
		analysisConfig, err := controllerconfig.NewCanaryAnalysisConfigCached(r.ConfigCache, r.Clientset)
		if err != nil {
			log.Error(err, "Failed to load canaryAnalysis config")
			return reconcile.Result{}, errors.Wrapf(err, "fails to load canaryAnalysis config")
		}
		var defaultReadyTimeout time.Duration
		if rolloutConfig != nil {
			defaultReadyTimeout = rolloutConfig.DefaultReadyTimeout
		}
		ra, err := canary.Dispatch(ctx, canary.DispatchDeps{
			ISVC:                     isvc,
			Client:                   r.Client,
			Reader:                   r.APIReader,
			Recorder:                 r.Recorder,
			Sampler:                  r.CanarySampler,
			BundledPrometheusAddress: analysisConfig.BundledPrometheusAddress,
			QueryTimeout:             analysisConfig.QueryTimeoutDuration(),
			DefaultReadyTimeout:      defaultReadyTimeout,
			MetricProviders:          metricProviders,
			DefaultProvider:          analysisConfig.DefaultProvider,
			ComponentRunnerPorts:     componentRunnerPorts,
		})
		if err != nil {
			log.Error(err, "Failed to reconcile canary rollout")
			return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile canary")
		}
		if ra > pendingRequeue.RequeueAfter {
			pendingRequeue.RequeueAfter = ra
		}
	}
	// Load the coordination tuning per-reconcile from the operator config so
	// ConfigMap edits take effect within the cache TTL without a restart (mirrors
	// canaryAnalysis). An absent "coordination" key yields a zero deadband (no
	// hysteresis), preserving the prior write-on-any-diff behavior.
	coordinationConfig, err := controllerconfig.NewCoordinationConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.Error(err, "Failed to load coordination config")
		return reconcile.Result{}, errors.Wrapf(err, "fails to load coordination config")
	}
	if _, err := coordination.Reconcile(ctx, coordination.ReconcileInputs{
		ISVC:                         isvc,
		Client:                       r.Client,
		Reader:                       r.APIReader,
		Recorder:                     r.Recorder,
		TrafficWeightDeadbandPercent: coordinationConfig.TrafficWeightDeadbandPercent,
		DefaultRatioTolerancePercent: coordinationConfig.DefaultRatioTolerancePercent,
		ComponentDeploymentModes:     componentDeploymentModes,
		ComponentRunnerPorts:         componentRunnerPorts,
	}); err != nil {
		log.Error(err, "Failed to reconcile cross-Component coordination")
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile coordination")
	}

	// If any Component asked to requeue, return that signal AFTER
	// coordination has run. Ingress / traffic / external-service
	// reconciliation downstream is skipped this pass — they re-run on
	// the requeue. This preserves the prior short-circuit semantics
	// for downstream reconcilers while ensuring coordination's
	// producer-side status writes (Traffic, RolloutCoordination) land
	// every pass. updateStatus must be called BEFORE the early return
	// — coordination.Reconcile above just mutated isvc.Status in-memory
	// (Traffic[], RolloutCoordination, RolloutCoordinationReady); without
	// flushing it here those writes are dropped on every requeuing pass,
	// which is the steady state during active rollouts.
	if pendingRequeue.Requeue || pendingRequeue.RequeueAfter > 0 {
		if err := r.updateStatus(isvc, deploymentMode); err != nil {
			r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
			return reconcile.Result{}, err
		}
		return pendingRequeue, nil
	}

	// Now reconcile ingress and external service after components have created their services
	ingressConfig, err := controllerconfig.NewIngressConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create IngressConfig")
	}

	// Resolve ingress config with annotation overrides
	resolvedIngressConfig := isvcutils.ResolveIngressConfig(ingressConfig, isvc.Annotations)

	// New architecture: ingress uses the determined ingress deployment mode
	ingressReconciler := ingress.NewIngressReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig, isvcConfig)
	log.V(1).Info("Reconciling ingress for inference service")
	if err := ingressReconciler.(*ingress.IngressReconciler).ReconcileWithDeploymentMode(ctx, isvc, ingressDeploymentMode); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile ingress")
	}

	// Reconcile external service - creates a service with the inference service name
	// when ingress is disabled to provide a stable endpoint
	externalServiceReconciler := external_service.NewExternalServiceReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig)
	log.V(1).Info("Reconciling external service for inference service")
	if err := externalServiceReconciler.Reconcile(ctx, isvc); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile external service")
	}

	// Traffic management reconciler. Skipped when the InferenceService
	// declares no traffic intent (spec.traffic or ome.io/* annotation).
	// The reconciler invokes the active translator (chosen at startup
	// by the factory), applies the emitted backend policy resource,
	// and returns the TrafficStatus to write back. A nil reconciler
	// means the controller was set up without traffic management
	// (legitimate for tests / minimal configurations).
	if r.TrafficReconciler != nil {
		targetRoutes := traffic.ComputeTargetHTTPRoutes(isvc, mergedDecoder != nil, mergedRouter != nil)
		trafficStatus, err := r.TrafficReconciler.Reconcile(ctx, isvc, targetRoutes)
		if err != nil {
			r.Log.Error(err, "Failed to reconcile traffic policy",
				"namespace", isvc.Namespace, "inferenceService", isvc.Name,
				"translator", r.TrafficReconciler.TranslatorName())
			r.Recorder.Event(isvc, v1.EventTypeWarning, "TrafficReconcileError", err.Error())
			// Surface the status even on error so operators see the
			// TranslationFailed reason without waiting for the next
			// successful reconcile.
			isvc.Status.Traffic = trafficStatus
			return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile traffic policy")
		}
		isvc.Status.Traffic = trafficStatus
	}

	// Set Status.Address for external service and add ingress disable annotation when ingress is disabled
	if resolvedIngressConfig.DisableIngressCreation {
		// Add annotation to InferenceService so cleanup logic knows to keep the external service
		if err := r.ensureIngressDisableAnnotation(isvc); err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "fails to add ingress disable annotation")
		}
		// A missing external service is not a failure. The reconciler above
		// creates one only for a non-cluster-local InferenceService that has
		// something to route to, and where it does create one, the read below
		// goes through the informer cache, which has not observed that write
		// yet. The Service is owned by this InferenceService and watched, so
		// its create event brings the reconcile back to set the URL. Returning
		// an error would add a backoff, a warning event and a stack trace to a
		// state that either resolves itself or is correct as it stands.
		if err := r.setExternalServiceURL(ctx, isvc, resolvedIngressConfig); err != nil {
			if !apierrors.IsNotFound(err) {
				r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
				return reconcile.Result{}, errors.Wrapf(err, "fails to set external service URL")
			}
			log.V(1).Info("external service absent; leaving the URL unset for this pass",
				"service", isvc.Name)
		}
	}

	// Clean up resources for components that no longer exist. Cleanup
	// errors don't fail reconciliation (we still want the rest of the
	// status flush to land), but we log Error so a chronic permission
	// failure on cleanup isn't fully invisible to the operator.
	if err := r.cleanupRemovedComponents(ctx, isvc, mergedEngine, mergedDecoder, mergedRouter); err != nil {
		log.Error(err, "Failed to cleanup removed components")
	}

	// Clean up status for components that no longer exist. Demoted to
	// V(1): this block runs every reconcile but in steady state every
	// declared Component still has a merged spec, so the "Cleaning up"
	// breadcrumb fires on the hot path without ever doing real work.
	// The per-Component delete lines stay at Info because they only
	// fire on a real spec change (Component removed from the ISVC).
	if isvc.Status.Components != nil {
		log.V(1).Info("Cleaning up component status",
			"mergedEngine", mergedEngine != nil,
			"mergedDecoder", mergedDecoder != nil,
			"mergedRouter", mergedRouter != nil,
			"statusComponents", len(isvc.Status.Components))

		if mergedEngine == nil {
			if _, present := isvc.Status.Components[v1beta1.EngineComponent]; present {
				delete(isvc.Status.Components, v1beta1.EngineComponent)
				log.Info("Deleted engine from status")
			}
		}
		if mergedDecoder == nil {
			if _, present := isvc.Status.Components[v1beta1.DecoderComponent]; present {
				delete(isvc.Status.Components, v1beta1.DecoderComponent)
				log.Info("Deleted decoder from status")
			}
		}
		if mergedRouter == nil {
			if _, present := isvc.Status.Components[v1beta1.RouterComponent]; present {
				delete(isvc.Status.Components, v1beta1.RouterComponent)
				log.Info("Deleted router from status")
			}
		}
	}

	// For every Component whose resolved deployment mode is OMENative
	// (i.e. flows through the IR-managed path), read the live IR.Status
	// and copy it onto ISVC.Status.Components[<component>].Lifecycle.
	// The IR controller is the sole writer of IR.Status. Components on
	// non-OMENative paths are not touched here; their status was already
	// written by the Raw / MultiNode reconciler during the per-Component
	// dispatcher pass above.
	//
	// Errors propagate so a transient apiserver read failure triggers
	// a retry; first-reconcile NotFound is swallowed inside the
	// helper (the next reconcile picks up the IR's fresh status once
	// the cache observes it).
	componentModes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent:  engineDeploymentMode,
		v1beta1.DecoderComponent: decoderDeploymentMode,
		v1beta1.RouterComponent:  routerDeploymentMode,
	}
	if err = irprojector.AggregateIRStatus(ctx, r.Client, r.APIReader, isvc, componentModes); err != nil {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	if err = r.updateStatus(isvc, deploymentMode); err != nil {
		// A terminal status conflict is benign — requeue and re-reconcile
		// off fresh state instead of surfacing an ERROR-level failure.
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

func validateResolvedRuntimeEnabled(runtimeSpec *v1beta1.ServingRuntimeSpec, runtimeName string, isCluster bool) error {
	if runtimeSpec != nil && runtimeSpec.IsDisabled() {
		return &runtimeselector.RuntimeDisabledError{
			RuntimeName: runtimeName,
			IsCluster:   isCluster,
		}
	}
	return nil
}

func (r *InferenceServiceReconciler) handleVirtualDeployment(isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	// We directly set URL and inference service status to Ready in VirtualDeployment mode

	// Honor the configured urlScheme rather than hardcoding http.
	ingressConfig, err := controllerconfig.NewIngressConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Set URL across all Status components
	host := network.GetServiceHostname(isvc.Name, isvc.Namespace)
	openAIURL := &knapis.URL{
		Host:   host,
		Scheme: ingressConfig.UrlScheme,
	}
	addressURL := &duckv1.Addressable{
		URL: &knapis.URL{
			Host:   host,
			Scheme: ingressConfig.UrlScheme,
		},
	}
	isvc.Status.URL = openAIURL
	isvc.Status.Address = addressURL
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			URL: openAIURL,
		},
	}

	isvc.Status.SetConditions(knapis.Conditions{{
		Type:               knapis.ConditionReady,
		Status:             v1.ConditionTrue,
		LastTransitionTime: knapis.VolatileTime{Inner: metav1.Now()},
		Reason:             "VirtualDeployment",
		Message:            "InferenceService is in VirtualDeployment mode",
	}})

	if err := r.updateStatus(isvc, constants.VirtualDeployment); err != nil {
		// A terminal status conflict is benign — requeue and re-reconcile
		// off fresh state instead of surfacing an ERROR-level failure.
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}

	return ctrl.Result{}, nil
}

// markRuntimeUnresolved handles the "runtime cannot be resolved" case — a
// named runtime that doesn't exist, or no compatible runtime for the model.
// It is deliberately NON-DESTRUCTIVE: the reconcile bails before the child
// create/update/delete, so a currently-serving ISVC keeps running its
// existing pods. The problem is surfaced as the advisory RuntimeReady=False
// condition (NOT a dependent of the aggregate Ready, so a healthy ISVC stays
// Ready=True) plus a Warning event, then status is persisted and the
// reconcile returns WITHOUT an error — so the controller does not
// hot-requeue or spam ERROR logs. The ServingRuntime / ClusterServingRuntime
// watch re-triggers reconcile when a matching runtime is created, so this
// self-heals.
func (r *InferenceServiceReconciler) markRuntimeUnresolved(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType, runtimeRef string, cause error) (reconcile.Result, error) {
	r.Log.Info("Runtime not found; awaiting runtime creation (ISVC left unchanged)",
		"runtime", runtimeRef, "InferenceService", isvc.Name)
	r.Recorder.Event(isvc, v1.EventTypeWarning, "RuntimeNotFound", cause.Error())
	isvc.Status.SetCondition(v1beta1.RuntimeReady, &knapis.Condition{
		Type:    v1beta1.RuntimeReady,
		Status:  v1.ConditionFalse,
		Reason:  "RuntimeNotFound",
		Message: cause.Error(),
	})
	if err := r.updateStatus(isvc, deploymentMode); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// clearRuntimeUnresolved marks the advisory RuntimeReady condition True once
// the runtime resolves — but only when it was previously set, so ISVCs that
// never hit a runtime problem don't gain a spurious condition (and don't
// churn status across an upgrade).
func (r *InferenceServiceReconciler) clearRuntimeUnresolved(isvc *v1beta1.InferenceService) {
	if isvc.Status.GetCondition(v1beta1.RuntimeReady) == nil {
		return
	}
	isvc.Status.SetCondition(v1beta1.RuntimeReady, &knapis.Condition{
		Type:   v1beta1.RuntimeReady,
		Status: v1.ConditionTrue,
		Reason: "RuntimeResolved",
	})
}

func (r *InferenceServiceReconciler) updateStatus(desiredService *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType) error {
	namespacedName := types.NamespacedName{Name: desiredService.Name, Namespace: desiredService.Namespace}

	// Mirror the existing-status snapshot OUTSIDE the retry loop —
	// "wasReady" is computed against what the caller observed at the
	// top of reconcile, which is the right baseline for the Ready /
	// NotReady transition event emission below. The retry loop only
	// needs to win the optimistic-concurrency race for the actual
	// status write.
	existingService := &v1beta1.InferenceService{}
	if err := r.Get(context.TODO(), namespacedName, existingService); err != nil {
		if apierrors.IsNotFound(err) {
			// ISVC was deleted between top-of-reconcile and the status
			// flush. The deletion path has already handled cleanup; an
			// in-flight reconcile racing the delete has nothing left to
			// write. Returning nil drops the work item cleanly instead
			// of triggering controller-runtime retries that saturate the
			// work queue and stall unrelated tests.
			return nil
		}
		return err
	}
	wasReady := inferenceServiceReadiness(existingService.Status)
	if inferenceServiceStatusEqual(existingService.Status, desiredService.Status) {
		// If we didn't change anything then don't call updateStatus.
		// This is important because the copy we loaded from the informer's
		// cache may be stale, and we don't want to overwrite a prior update
		// to status with this stale state.
		return nil
	}

	// Status().Update wrapped in retry.RetryOnConflict because
	// OMENative-driven status churn (drain / Mark / op transitions)
	// reliably 409s the outer flush and would wedge the reconcile in a
	// re-loop. The retry base is read through APIReader (the live
	// apiserver), NOT the informer cache: a metadata write earlier in the
	// same reconcile — e.g. canary promote-annotation consumption — bumps
	// the ResourceVersion before the cache observes it, so a cached re-Get
	// would hand back a stale base and 409 every attempt, silently dropping
	// this status write. On conflict: re-Get the live ISVC, copy
	// desiredService status onto the fresh ResourceVersion, then re-merge
	// ComponentStatus.Lifecycle from latest so any OMENative writes that
	// landed between our snapshot and the re-Get (Incarnation bumps,
	// Phase transitions, RunningRevision promotes) are preserved. The
	// outer reconciler doesn't own that subtree.
	//
	// Readiness of the object this flush actually wrote over, captured from
	// the live apiserver read on the attempt that wins. The event baseline
	// above deliberately uses the cached top-of-reconcile snapshot; the
	// time-to-ready metric needs the durable one, because the IR projector
	// stamps component-ready conditions on the same object outside this
	// function and can flip Ready between the two reads.
	var preWriteReady *knapis.Condition
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1beta1.InferenceService{}
		if err := r.APIReader.Get(context.TODO(), namespacedName, latest); err != nil {
			return err
		}
		preWriteReady = latest.Status.GetCondition(knapis.ConditionReady)
		preserved := preserveLifecycleStatus(latest.Status.Components)
		// Snapshot the live run state before the overwrite: the rollout
		// run's pinned plan must never move BACKWARD under a writer that
		// reconciled from a stale cache (a rolled-back pin is unrecoverable
		// — its repin verb was one-shot). The reconciler doesn't own that
		// subtree any more than it owns Lifecycle when another pass's write
		// is newer.
		liveRollout := latest.Status.Rollout.DeepCopy()
		livePreStepHold := latest.Status.Canary != nil && latest.Status.Canary.PreStepHold
		latest.Status = desiredService.Status
		mergeLifecycleStatus(&latest.Status, preserved)
		rolloutrun.PreserveNewerRun(&latest.Status, liveRollout, livePreStepHold)
		return r.Status().Update(context.TODO(), latest)
	}); err != nil {
		// Same race as the top-of-function Get: if the ISVC vanished
		// during the retry loop, there's nothing to update — drop the
		// work item silently. Only log + retry on other errors.
		if apierrors.IsNotFound(err) {
			obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultNotFound)
			return nil
		}
		// Exhausted RetryOnConflict still surfaces a Conflict error —
		// bucket it separately so the OMENative 409 churn hotspot stays
		// visible apart from other write failures.
		if apierrors.IsConflict(err) {
			obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultConflict)
			// Benign: a concurrent writer bumped the ISVC. The next
			// reconcile re-reads fresh state, so log at V(1) and surface the
			// conflict for the caller to map to a requeue rather than an
			// ERROR-level "Reconciler error".
			r.Log.V(1).Info("InferenceService status write lost an optimistic-lock race; requeueing",
				"InferenceService", desiredService.Name)
			return errors.Wrapf(err, "fails to update InferenceService status")
		}
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultError)
		r.Log.Error(err, "Failed to update InferenceService status", "InferenceService", desiredService.Name)
		r.Recorder.Eventf(desiredService, v1.EventTypeWarning, "UpdateFailed",
			"Failed to update status for InferenceService %q: %v", desiredService.Name, err)
		return errors.Wrapf(err, "fails to update InferenceService status")
	}
	obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultSuccess)

	// Ready / NotReady transition events fire only after a successful
	// status write that actually flipped readiness.
	isReady := inferenceServiceReadiness(desiredService.Status)
	if isReady && (preWriteReady == nil || preWriteReady.Status != v1.ConditionTrue) {
		isvcstatus.ObserveTimeToReady(desiredService, preWriteReady)
	}
	if wasReady && !isReady {
		r.Recorder.Eventf(desiredService, v1.EventTypeWarning, string(InferenceServiceNotReadyState),
			"InferenceService [%v] is no longer Ready", desiredService.GetName())
	} else if !wasReady && isReady {
		r.Recorder.Eventf(desiredService, v1.EventTypeNormal, string(InferenceServiceReadyState),
			"InferenceService [%v] is Ready", desiredService.GetName())
	}
	return nil
}

func inferenceServiceReadiness(status v1beta1.InferenceServiceStatus) bool {
	return status.Conditions != nil &&
		status.GetCondition(knapis.ConditionReady) != nil &&
		status.GetCondition(knapis.ConditionReady).Status == v1.ConditionTrue
}

func inferenceServiceStatusEqual(s1, s2 v1beta1.InferenceServiceStatus) bool {
	return equality.Semantic.DeepEqual(s1, s2)
}

// preserveLifecycleStatus snapshots the OMENative ComponentStatus
// subtree per Component, so the caller can re-merge it after overwriting
// latest.Status with the reconcile-pass desired status. The OMENative
// reconciler owns Status.Components[*].Lifecycle; the outer reconciler
// only touches URL / RestURL / Address / RolloutPhase / Conditions.
// Without re-merging, OMENative writes that landed between our snapshot
// and the conflict re-Get get clobbered.
func preserveLifecycleStatus(comps map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec) map[v1beta1.ComponentType]*v1beta1.LifecycleStatus {
	if len(comps) == 0 {
		return nil
	}
	out := make(map[v1beta1.ComponentType]*v1beta1.LifecycleStatus, len(comps))
	for c, cs := range comps {
		if cs.Lifecycle != nil {
			out[c] = cs.Lifecycle.DeepCopy()
		}
	}
	return out
}

func mergeLifecycleStatus(s *v1beta1.InferenceServiceStatus, preserved map[v1beta1.ComponentType]*v1beta1.LifecycleStatus) {
	if len(preserved) == 0 {
		return
	}
	if s.Components == nil {
		s.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	for c, om := range preserved {
		cs := s.Components[c]
		cs.Lifecycle = om
		s.Components[c] = cs
	}
}

// ensureIngressDisableAnnotation adds the ome.io/ingress-disable-creation annotation to the InferenceService
// This annotation is used by the cleanup logic to determine if external service should be kept
func (r *InferenceServiceReconciler) ensureIngressDisableAnnotation(isvc *v1beta1.InferenceService) error {
	const ingressDisableAnnotation = "ome.io/ingress-disable-creation"

	// Check if annotation already exists
	if val, ok := isvc.Annotations[ingressDisableAnnotation]; ok && val == "true" {
		return nil
	}

	// Add annotation
	if isvc.Annotations == nil {
		isvc.Annotations = make(map[string]string)
	}
	isvc.Annotations[ingressDisableAnnotation] = "true"

	return nil
}

func (r *InferenceServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.ClientConfig = mgr.GetConfig()
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if err := r.validateWiring(); err != nil {
		return err
	}
	if r.Expectations == nil {
		r.Expectations = omenative.NewExpectations()
	}
	// scheduler-plugins PodGroup detection. Optional CRD; absence is a
	// degradation surface, not a hard fail. OMENative's reconciler reads
	// the cached flag via ReconcileParams.GangSchedulingAvailable and
	// stamps a `GangSchedulingUnavailable=True` Component Condition when
	// a multi-pod Component is requested without the CRD.
	podGroupFound, err := utils.IsCrdAvailable(r.ClientConfig, schedulingv1alpha1.SchemeGroupVersion.String(), constants.PodGroupKind)
	if err != nil {
		return err
	}
	r.GangSchedulingAvailable = podGroupFound
	r.componentDeps = &components.ComponentDeps{
		Client:                  r.Client,
		Clientset:               r.Clientset,
		APIReader:               r.APIReader,
		Expectations:            r.Expectations,
		Recorder:                r.Recorder,
		Scheme:                  r.Scheme,
		GangSchedulingAvailable: r.GangSchedulingAvailable,
	}
	// Short-TTL cache for the inferenceservice-config ConfigMap so a reconcile
	// pass shares one apiserver GET across the per-pass config loads. TTL is
	// flag-driven; a non-positive TTL disables caching (reads the apiserver every
	// call), preserving the pre-cache behavior.
	if r.ConfigCache == nil {
		r.ConfigCache = controllerconfig.NewConfigCache(r.ConfigCacheTTL)
	}

	// Register the spec.runtime.name field index so runtime change events
	// fan out to referencing ISVCs through the cache index instead of
	// scanning every cached InferenceService — see
	// isvcsReferencingRuntime.
	if err := registerISVCRuntimeNameIndex(context.Background(), mgr.GetFieldIndexer()); err != nil {
		return err
	}

	// Initialize RuntimeSelector
	r.RuntimeSelector = runtimeselector.New(mgr.GetClient())

	// Initialize AcceleratorClassSelector
	r.AcceleratorClassSelector = acceleratorclassselector.New(mgr.GetClient())

	// KEDA is an optional CRD: probe and only watch ScaledObject when present.
	// An unconditional watch fails manager startup (cache-sync timeout) when the
	// CRD is absent, crash-looping the controller for every ISVC. A class=keda
	// ISVC still fails loudly on its own reconcile when KEDA is missing.
	kedaFound, err := utils.IsCrdAvailable(r.ClientConfig, kedav1.SchemeGroupVersion.String(), constants.KEDAScaledObjectKind)
	if err != nil {
		return err
	}
	r.KedaAvailable = kedaFound

	// AutoscalerPolicy is chart-gated together with its controller wiring and
	// webhook (never install-but-dormant): probe the CRD so a cluster without
	// the feature registers no watch or index, and refs fail closed at
	// reconcile instead of silently no-oping.
	autoscalerPolicyFound, err := utils.IsCrdAvailable(r.ClientConfig, v1beta1.SchemeGroupVersion.String(), constants.AutoscalerPolicyKind)
	if err != nil {
		return err
	}
	r.AutoscalerPolicyEnabled = autoscalerPolicyFound

	// RolloutPolicy follows the same chart-gated pattern: no CRD, no watch,
	// no index — stored refs then park at run open rather than no-op.
	rolloutPolicyFound, err := utils.IsCrdAvailable(r.ClientConfig, v1beta1.SchemeGroupVersion.String(), constants.RolloutPolicyKind)
	if err != nil {
		return err
	}
	r.RolloutPolicyEnabled = rolloutPolicyFound

	podMonitorFound, err := utils.IsCrdAvailable(r.ClientConfig, monitoringv1.SchemeGroupVersion.String(), constants.PodMonitorKind)
	if err != nil {
		return err
	}

	// LeaderWorkerSet is an optional CRD: probe and only watch it when
	// present, so clusters without LWS keep working (MultiNode dispatch
	// then fails loudly on its own reconcile).
	lwsFound, err := utils.IsCrdAvailable(r.ClientConfig, lws.SchemeGroupVersion.String(), constants.LWSKind)
	if err != nil {
		return err
	}

	// Build the canary analysis sampler once. Its concurrency cap and cache TTL are
	// fixed for the controller's lifetime (the per-sample source + query timeout are
	// resolved per reconcile from the same config). A missing/unreadable config
	// falls back to the documented defaults so the sampler is always available — a
	// genuinely broken ConfigMap surfaces per-reconcile when a canary needs it.
	analysisConfig, err := controllerconfig.NewCanaryAnalysisConfig(r.Clientset)
	if err != nil {
		r.Log.Info("canaryAnalysis config unavailable at startup; using sampler defaults", "error", err.Error())
		analysisConfig = &controllerconfig.CanaryAnalysisConfig{MaxConcurrency: controllerconfig.DefaultAnalysisMaxConcurrency}
	}
	// The wake-up channel buffer is sized to the query concurrency: at most that
	// many queries finish near-simultaneously, and a full buffer harmlessly drops a
	// wake-up (the reconcile requeue is the backstop), so it never blocks a query.
	analysisEvents := make(chan event.GenericEvent, analysisConfig.MaxConcurrency)
	r.CanarySampler = canary.NewPrometheusSampler(analysisEvents, int(analysisConfig.MaxConcurrency), analysisConfig.CacheTTLDuration())

	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		// MaxConcurrentReconciles parallelizes reconciles for distinct ISVCs;
		// zero (unset) falls back to controller-runtime's single-worker default.
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		// Ignore the ISVC's own status-only writes (which bump resourceVersion
		// without touching spec/generation or metadata). Still trigger on spec
		// (generation) changes AND on label/annotation changes — OME drives
		// canary promote/rollback via annotations that do NOT bump generation,
		// so a blanket GenerationChangedPredicate would break rollouts.
		For(&v1beta1.InferenceService{}, builder.WithPredicates(isvcReconcileTriggerPredicate())).
		Owns(&appsv1.Deployment{}).
		Owns(&v1.Service{}).
		Owns(&v1.ConfigMap{}).
		Owns(&v1.PersistentVolume{}).
		Owns(&v1.PersistentVolumeClaim{}).
		// Ignore HPA status-only churn. The HPA controller perpetually
		// rewrites .status.conditions when a metric can't be fetched
		// (e.g. CPU-utilization HPA on pods with no CPU request); without
		// this filter every such rewrite re-reconciles the ISVC.
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}, builder.WithPredicates(ownedStatusIgnoringPredicate())).
		Owns(&policyv1.PodDisruptionBudget{}).
		// OMENative revision history is owned by the ISVC
		// so revision-driven status writes enqueue the parent.
		Owns(&appsv1.ControllerRevision{}).
		// InferenceReplica is owned by the ISVC controller —
		// the projector at the dispatch site (irprojector.EnsureInferenceReplica)
		// stamps the ISVC as the IR's controller OwnerReference so
		// GC cascades on ISVC delete, and Owns() re-enqueues
		// the parent ISVC when the IR controller flips IR.Status
		// (Replicas / ReadyReplicas / phase progression). This is what
		// lets the ISVC status aggregator pick up IR-side counter
		// updates without polling.
		Owns(&v1beta1.InferenceReplica{}).
		// EndpointSlice events for OMENative headless Services enqueue
		// the parent ISVC so drain checks (IsPodDrained / IsPodInRotation)
		// react to kube-proxy convergence without waiting on the periodic
		// requeue. The mapper rejects slices that don't target an
		// OMENative-managed Service.
		Watches(
			&discoveryv1.EndpointSlice{},
			handler.EnqueueRequestsFromMapFunc(omenative.EndpointSliceToISVC),
		)

	// Owned only when the CRD is present; ignore status-only churn like the HPA.
	if kedaFound {
		ctrlBuilder = ctrlBuilder.Owns(&kedav1.ScaledObject{}, builder.WithPredicates(ownedStatusIgnoringPredicate()))
	} else {
		r.Log.Info("The InferenceService controller won't watch keda.sh/v1alpha1/ScaledObject resources because the CRD is not available; InferenceServices requesting KEDA autoscaling will fail on reconcile until KEDA is installed.")
	}

	if r.AutoscalerPolicyEnabled {
		// One field index carries every distinct per-component ref name, so a
		// policy event fans out to exactly its same-namespace consumers.
		if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &v1beta1.InferenceService{}, autoscalerPolicyRefIndexField, autoscalerPolicyRefIndexer); err != nil {
			return err
		}
		ctrlBuilder = ctrlBuilder.Watches(&v1beta1.AutoscalerPolicy{}, handler.EnqueueRequestsFromMapFunc(r.mapAutoscalerPolicyToInferenceServices))
	} else {
		r.Log.Info("The InferenceService controller won't watch ome.io/v1beta1/AutoscalerPolicy resources because the CRD is not available; autoscalerPolicyRef attachments fail closed until the feature is installed.")
	}

	if r.RolloutPolicyEnabled {
		// One index over the distinct per-group ref names powers the
		// policy-to-consumers fan-out (a parked plan recovers watch-driven
		// when its policy appears) and the status controller's attached-group
		// accounting.
		if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &v1beta1.InferenceService{}, rolloutpolicycontroller.PolicyRefIndexField, rolloutpolicycontroller.PolicyRefIndexer); err != nil {
			return err
		}
		ctrlBuilder = ctrlBuilder.Watches(&v1beta1.RolloutPolicy{}, handler.EnqueueRequestsFromMapFunc(r.mapRolloutPolicyToInferenceServices))
	} else {
		r.Log.Info("The InferenceService controller won't watch ome.io/v1beta1/RolloutPolicy resources because the CRD is not available; rollout policyRef attachments park at run open until the feature is installed.")
	}

	if podMonitorFound {
		ctrlBuilder = ctrlBuilder.Owns(&monitoringv1.PodMonitor{})
	} else {
		r.Log.Info("The InferenceService controller won't watch monitoring.coreos.com/v1/PodMonitor resources because the CRD is not available.")
	}

	if lwsFound {
		ctrlBuilder = ctrlBuilder.Owns(&lws.LeaderWorkerSet{})
	} else {
		r.Log.Info("The InferenceService controller won't watch leaderworkerset.x-k8s.io/v1/LeaderWorkerSet resources because the CRD is not available.")
	}

	if podGroupFound {
		// OMENative-emitted PodGroups carry the ISVC as controller-owner;
		// owner-ref-based enqueue re-reconciles the ISVC when a PodGroup
		// is modified or deleted out from under us.
		ctrlBuilder = ctrlBuilder.Owns(&schedulingv1alpha1.PodGroup{})
	} else {
		r.Log.Info("The InferenceService controller won't watch scheduling.x-k8s.io/v1alpha1/PodGroup resources because the CRD is not available; OMENative multi-pod Instances will degrade to non-gang scheduling.")
	}

	// Gateway-API HTTPRoutes are owned by the ISVC, and IngressReady gates
	// on the route's parent (Gateway) programming status. Watch them so a
	// gateway flipping a route to programmed re-reconciles the ISVC —
	// otherwise IngressReady=False would persist until an unrelated event.
	// Scheme registration is conditional on EnableGatewayAPI (see
	// cmd/manager), so gate on both the scheme and the CRD being present.
	const httpRouteKind = "HTTPRoute"
	// gateway-api exports its group/version as a metav1.GroupVersion, which
	// carries no WithKind; the two structs are field-identical, so convert.
	gatewayGV := schema.GroupVersion(gatewayapiv1.GroupVersion)
	if mgr.GetScheme().Recognizes(gatewayGV.WithKind(httpRouteKind)) {
		httpRouteFound, err := utils.IsCrdAvailable(r.ClientConfig, gatewayGV.String(), httpRouteKind)
		if err != nil {
			return err
		}
		if httpRouteFound {
			ctrlBuilder = ctrlBuilder.Owns(&gatewayapiv1.HTTPRoute{})
		} else {
			r.Log.Info("The InferenceService controller won't watch gateway.networking.k8s.io/v1/HTTPRoute resources because the CRD is not available.")
		}
	}

	// Register an Owns() watch on the backend policy resource type
	// the active translator emits, so changes to the policy re-enqueue
	// the owning InferenceService. The noop translator returns nil, in
	// which case there is nothing to watch.
	if r.TrafficReconciler != nil {
		if watch := r.TrafficReconciler.Watches(); watch != nil {
			ctrlBuilder = ctrlBuilder.Owns(watch)
		} else {
			r.Log.Info("The InferenceService controller won't watch a backend policy resource (active translator emits nothing)",
				"translator", r.TrafficReconciler.TranslatorName())
		}
	}

	// Add watches for ServingRuntime and ClusterServingRuntime. Runtime
	// events fan out to EVERY ISVC referencing the runtime: autoSync=true
	// (float) ISVCs re-render from the live runtime and roll forward;
	// autoSync=false (pinned) ISVCs run drift detection and warn. Without
	// this fan-out a runtime change never triggers a float ISVC's reconcile,
	// so the edit silently fails to propagate — see isvcsReferencingRuntime.
	ctrlBuilder = ctrlBuilder.
		Watches(&v1beta1.ServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(r.isvcsReferencingRuntime)).
		Watches(&v1beta1.ClusterServingRuntime{},
			handler.EnqueueRequestsFromMapFunc(r.isvcsReferencingRuntime))

	// AcceleratorClass events fan out to every ISVC with an accelerator
	// preference. Policy-based selection (BestFit/Cheapest/...) can resolve
	// to ANY class, so the mapping is preference-bearing ISVCs, not ISVCs
	// naming this class. Without this watch a class edit (or the class
	// finally appearing) never re-reconciles an ISVC that selected — or
	// failed to select — an accelerator.
	ctrlBuilder = ctrlBuilder.
		Watches(&v1beta1.AcceleratorClass{},
			handler.EnqueueRequestsFromMapFunc(r.isvcsWithAcceleratorPreference),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}))

	// Re-reconcile an ISVC when its canary analysis sample lands, so the executor
	// consumes the result immediately instead of waiting on the periodic requeue.
	// The sampler emits a GenericEvent carrying the ISVC's namespace/name.
	ctrlBuilder = ctrlBuilder.WatchesRawSource(
		source.Channel(analysisEvents, &handler.EnqueueRequestForObject{}))

	return ctrlBuilder.Complete(r)
}

// isvcRuntimeNameIndexField is the controller-runtime cache field-index
// name keyed on an ISVC's spec.runtime.name. Without it, a runtime
// change event lists every cached InferenceService and scans for those
// referencing the runtime; with it MatchingFields narrows the list to
// the referencing ISVCs in O(matched). Registered on the manager cache
// in SetupWithManager (see registerISVCRuntimeNameIndex).
//
// The field name is an internal index identifier (a code constant), not
// a behavioral/user-facing value — no config surface is required.
const isvcRuntimeNameIndexField = "spec.runtime.name"

// isvcRuntimeNameIndexExtractor is the cache IndexerFunc registered for
// isvcRuntimeNameIndexField. It returns the ISVC's spec.runtime.name, or
// nil when the runtime ref (or its name) is absent — the unindexed
// match below skips those identically.
func isvcRuntimeNameIndexExtractor(obj client.Object) []string {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil
	}
	if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name == "" {
		return nil
	}
	return []string{isvc.Spec.Runtime.Name}
}

// isvcRuntimeUnresolvedIndexField indexes auto-select ISVCs (no
// spec.runtime.name) currently stuck with RuntimeReady=False. The name
// index above cannot cover them — they reference no runtime by name — so
// without this index a runtime created AFTER the ISVC never re-triggers
// its reconcile and markRuntimeUnresolved becomes an absorbing state.
// Internal index identifier, like isvcRuntimeNameIndexField.
const isvcRuntimeUnresolvedIndexField = "status.runtimeUnresolved"

// isvcRuntimeUnresolvedIndexValue is the single bucket value for
// isvcRuntimeUnresolvedIndexField (membership index, not a lookup key).
const isvcRuntimeUnresolvedIndexValue = "true"

func isvcRuntimeUnresolvedIndexExtractor(obj client.Object) []string {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil
	}
	if isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != "" {
		// Named references fan out via the name index.
		return nil
	}
	cond := isvc.Status.GetCondition(v1beta1.RuntimeReady)
	if cond == nil || cond.Status != v1.ConditionFalse {
		return nil
	}
	return []string{isvcRuntimeUnresolvedIndexValue}
}

// registerISVCRuntimeNameIndex installs the runtime fan-out indexes on the
// supplied indexer (mgr.GetFieldIndexer()). Call once during manager
// setup, before Start, so isvcsReferencingRuntime resolves
// referencing ISVCs through the indexes instead of scanning every cached
// InferenceService.
func registerISVCRuntimeNameIndex(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &v1beta1.InferenceService{}, isvcRuntimeNameIndexField, isvcRuntimeNameIndexExtractor); err != nil {
		return err
	}
	return indexer.IndexField(ctx, &v1beta1.InferenceService{}, isvcRuntimeUnresolvedIndexField, isvcRuntimeUnresolvedIndexExtractor)
}

// isvcsReferencingRuntime returns reconcile requests for EVERY ISVC whose
// spec.runtime.name matches obj (respecting namespaced-SR scope), regardless
// of autoSync. Both float and pinned consumers must be re-reconciled on a
// runtime change:
//
//   - autoSync=true (default, "float"): the reconcile re-renders the pod
//     spec from the LIVE runtime, so a runtime edit rolls the ISVC forward.
//     Fanning these out is the whole point — without it a runtime change
//     never triggers the float ISVC's reconcile, so it silently fails to
//     propagate until some unrelated event happens to wake the ISVC. That
//     used to be masked by frequent incidental reconciles; once those were
//     trimmed (event-filter predicates, scoped informers), runtime edits
//     stopped reaching float ISVCs entirely.
//
//   - autoSync=false ("pinned"): the reconcile runs drift detection and
//     warns the operator that the runtime edit was rejected; it does not roll.
//
// NOTE: fanning out float ISVCs means a runtime edit rolls them. For
// RawDeployment that is a safe native rolling update; for OMENative it
// rolls per the Component's updateStrategy — an OMENative ISVC on
// RecreatePod with no rollout budget is recreated wholesale.
func (r *InferenceServiceReconciler) isvcsReferencingRuntime(ctx context.Context, obj client.Object) []reconcile.Request {
	runtimeName := obj.GetName()
	runtimeNamespace := obj.GetNamespace() // empty for cluster-scoped

	// Narrow to ISVCs whose spec.runtime.name matches via the field index;
	// the in-memory kind/namespace-scope filter below still applies.
	var isvcs v1beta1.InferenceServiceList
	if err := r.List(ctx, &isvcs, client.MatchingFields{isvcRuntimeNameIndexField: runtimeName}); err != nil {
		r.Log.Error(err, "fan-out runtime event: list InferenceServices failed")
		return nil
	}

	var reqs []reconcile.Request
	for i := range isvcs.Items {
		isvc := &isvcs.Items[i]
		if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name != runtimeName {
			continue
		}
		// Namespaced SR only matches same-namespace ISVCs; a cluster-scoped
		// runtime (empty namespace) matches ISVCs in any namespace.
		if runtimeNamespace != "" && isvc.Namespace != runtimeNamespace {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: isvc.Namespace, Name: isvc.Name,
		}})
	}

	// Also wake auto-select ISVCs parked on RuntimeReady=False: the new or
	// edited runtime may be the compatible one they are waiting for, and no
	// name reference exists to fan them out through the index above.
	var unresolved v1beta1.InferenceServiceList
	if err := r.List(ctx, &unresolved, client.MatchingFields{isvcRuntimeUnresolvedIndexField: isvcRuntimeUnresolvedIndexValue}); err != nil {
		r.Log.Error(err, "fan-out runtime event: list unresolved InferenceServices failed")
		return reqs
	}
	for i := range unresolved.Items {
		isvc := &unresolved.Items[i]
		if runtimeNamespace != "" && isvc.Namespace != runtimeNamespace {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: isvc.Namespace, Name: isvc.Name,
		}})
	}
	return reqs
}

// isvcsWithAcceleratorPreference returns reconcile requests for every ISVC
// that expresses any accelerator preference (spec selector, per-component
// override, or the accelerator-class annotation). AcceleratorClass events
// are rare (hardware catalog changes), so a full list scan is acceptable.
func (r *InferenceServiceReconciler) isvcsWithAcceleratorPreference(ctx context.Context, _ client.Object) []reconcile.Request {
	var isvcs v1beta1.InferenceServiceList
	if err := r.List(ctx, &isvcs); err != nil {
		r.Log.Error(err, "fan-out accelerator class event: list InferenceServices failed")
		return nil
	}

	var reqs []reconcile.Request
	for i := range isvcs.Items {
		isvc := &isvcs.Items[i]
		hasPreference := isvc.Spec.AcceleratorSelector != nil ||
			(isvc.Spec.Engine != nil && isvc.Spec.Engine.AcceleratorOverride != nil) ||
			(isvc.Spec.Decoder != nil && isvc.Spec.Decoder.AcceleratorOverride != nil)
		if !hasPreference {
			if _, ok := isvc.Annotations[constants.AcceleratorClassAnnotationKey]; !ok {
				continue
			}
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: isvc.Namespace, Name: isvc.Name,
		}})
	}
	return reqs
}

// setExternalServiceURL points the InferenceService's URL and Address at the
// Service that fronts it when ingress creation is disabled.
//
// The caller separates a missing Service from a real failure by asking
// apierrors.IsNotFound, so the lookup's error has to reach it as an API status
// error. Adding context around it is safe; replacing it with a plain error is
// not, and would make every absent Service fatal again.
func (r *InferenceServiceReconciler) setExternalServiceURL(ctx context.Context, isvc *v1beta1.InferenceService, ingressConfig *controllerconfig.IngressConfig) error {
	// Get the external service
	externalService := &v1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, externalService); err != nil {
		return err
	}

	// Get the port from the external service
	var port int32 = constants.CommonISVCPort // default port
	if len(externalService.Spec.Ports) > 0 {
		port = externalService.Spec.Ports[0].Port
	}

	// Set the URL and Address of the external service with port
	host := network.GetServiceHostname(externalService.Name, externalService.Namespace)
	hostWithPort := fmt.Sprintf("%s:%d", host, port)

	// Honor the configured urlScheme rather than hardcoding http.
	isvc.Status.URL = &knapis.URL{
		Host:   hostWithPort,
		Scheme: ingressConfig.UrlScheme,
	}
	isvc.Status.Address = &duckv1.Addressable{
		URL: &knapis.URL{
			Host:   hostWithPort,
			Scheme: ingressConfig.UrlScheme,
		},
	}

	return nil
}

// setOverlaysReadyCondition flips OverlaysReady=True when every
// declared overlay was attached; False with reasons in the Message
// when one or more were skipped. Informational — the primary still
// drives the deployment regardless.
func setOverlaysReadyCondition(isvc *v1beta1.InferenceService, overlays []isvcutils.ResolvedOverlay) {
	if isvc.Spec.Model == nil || len(isvc.Spec.Model.Overlays) == 0 {
		return
	}
	cond := knapis.Condition{
		Type: v1beta1.OverlaysReady,
	}
	skipped := components.SkippedOverlayReasons(overlays)
	if len(skipped) == 0 {
		cond.Status = v1.ConditionTrue
		cond.Reason = "AllOverlaysMounted"
	} else {
		cond.Status = v1.ConditionFalse
		cond.Reason = "OverlaysSkipped"
		cond.Message = strings.Join(skipped, "; ")
	}
	// Route through the condition manager: a raw SetConditions here would
	// replace the whole conditions slice and restamp transition times every
	// pass, forcing a status write per reconcile.
	isvc.Status.SetCondition(v1beta1.OverlaysReady, &cond)
}

// buildComponentDeps returns the component dependencies for one
// reconcile pass: the process-lifetime prototype plus the per-reconcile
// isvc config (the only non-static field).
func (r *InferenceServiceReconciler) buildComponentDeps(cfg *controllerconfig.InferenceServicesConfig) *components.ComponentDeps {
	// nil prototype: Reconcile invoked without SetupWithManager (tests).
	if r.componentDeps == nil {
		return &components.ComponentDeps{
			Client:                  r.Client,
			Clientset:               r.Clientset,
			APIReader:               r.APIReader,
			Expectations:            r.Expectations,
			Recorder:                r.Recorder,
			Scheme:                  r.Scheme,
			GangSchedulingAvailable: r.GangSchedulingAvailable,
			Config:                  cfg,
		}
	}
	d := *r.componentDeps
	d.Config = cfg
	return &d
}

// validateWiring rejects a mis-wired reconciler at setup. The
// authoritative (live) reader is a correctness dependency — see
// workload/types AuthoritativeReader.
func (r *InferenceServiceReconciler) validateWiring() error {
	if r.APIReader == nil {
		return fmt.Errorf("inferenceservice: APIReader (AuthoritativeReader) must be wired")
	}
	return nil
}

// mapRolloutPolicyToInferenceServices enqueues every same-namespace consumer
// of a changed RolloutPolicy — how a parked plan recovers without polling
// when its missing policy appears, and how drift conditions refresh on a
// policy edit.
func (r *InferenceServiceReconciler) mapRolloutPolicyToInferenceServices(ctx context.Context, obj client.Object) []reconcile.Request {
	consumers := &v1beta1.InferenceServiceList{}
	if err := r.Client.List(ctx, consumers,
		client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{rolloutpolicycontroller.PolicyRefIndexField: obj.GetName()}); err != nil {
		r.Log.Error(err, "Failed to list RolloutPolicy consumers", "policy", obj.GetName(), "namespace", obj.GetNamespace())
		return nil
	}
	requests := make([]reconcile.Request, 0, len(consumers.Items))
	for i := range consumers.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: consumers.Items[i].Namespace, Name: consumers.Items[i].Name},
		})
	}
	return requests
}

// autoscalerPolicyRefIndexField indexes InferenceServices by the distinct
// AutoscalerPolicy names their components reference, powering the
// policy-to-consumers watch fan-out and the attached-component accounting.
const autoscalerPolicyRefIndexField = "spec.autoscalerPolicyRefs"

func autoscalerPolicyRefIndexer(obj client.Object) []string {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	var names []string
	for _, componentType := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		ref := autoscaler.ComponentPolicyRef(isvc, componentType)
		if ref == nil || ref.Name == "" || seen[ref.Name] {
			continue
		}
		seen[ref.Name] = true
		names = append(names, ref.Name)
	}
	return names
}

// mapAutoscalerPolicyToInferenceServices enqueues every same-namespace
// consumer of a changed policy. Fan-out is bounded by consumers-per-namespace;
// each enqueued reconcile is an ordinary pass that re-renders and updates the
// ScaledObject in place.
func (r *InferenceServiceReconciler) mapAutoscalerPolicyToInferenceServices(ctx context.Context, obj client.Object) []reconcile.Request {
	consumers := &v1beta1.InferenceServiceList{}
	if err := r.Client.List(ctx, consumers, client.InNamespace(obj.GetNamespace()), client.MatchingFields{autoscalerPolicyRefIndexField: obj.GetName()}); err != nil {
		r.Log.Error(err, "failed to map AutoscalerPolicy to consumers", "policy", client.ObjectKeyFromObject(obj))
		return nil
	}
	requests := make([]reconcile.Request, 0, len(consumers.Items))
	for i := range consumers.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&consumers.Items[i])})
	}
	return requests
}
