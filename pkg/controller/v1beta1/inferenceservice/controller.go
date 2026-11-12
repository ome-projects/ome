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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/scaling"
	traffic "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	isvcstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/crdskew"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	rolloutpolicycontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils"
	"sigs.k8s.io/ome/pkg/validation"
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
// +kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=get
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
	// InstanceStatusDecoder decodes the per-Instance representation of the
	// InferenceReplica statuses the cross-Component coordination reads, under
	// the operator-configured ColumnarV2 row bound. The zero value carries no
	// bound: DenseV1 decodes unchanged and any ColumnarV2 object fails closed.
	InstanceStatusDecoder irstatus.Decoder
	// CRDSchema is the manager's picture of the installed CRDs against this
	// release's schema, written on every service as the CRDSchemaCurrent
	// condition; nil when nothing compared them.
	CRDSchema *crdskew.Report
	Log       logr.Logger
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	// Clock measures spec.ttlSecondsAfterCreation. Nil means the real clock.
	Clock           clock.PassiveClock
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
	// QuotaAcceleratorResources are the resource names whose presence in a
	// Component's pods puts it under the quota backend (--accelerator-resources).
	// A Component requesting none of them is projected without the Kueue
	// queue-name label, so its pods are never gated. Empty governs everything.
	QuotaAcceleratorResources []string
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

// shutdownCutShort reports whether err came from a pass the manager's own
// shutdown stopped. The next manager re-queues the object and finishes the
// work, so such an error is not a failure of the service and earns no report.
func shutdownCutShort(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled)
}

func (r *InferenceServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, req.NamespacedName, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			// A deleted service has nothing to reconcile: the objects it
			// owned are garbage collected.
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	// Every log line of the pass, and every callback that takes its logger
	// from ctx, carries the service's namespace and name.
	log := r.Log.WithValues("namespace", isvc.Namespace, "isvc", isvc.Name)
	pass := &servicePass{reconciler: r, isvc: isvc, log: log}
	return pass.run(ctrl.LoggerInto(ctx, log))
}

// servicePass is one reconcile pass over one InferenceService: what the pass
// read at its start and what its steps decide as they run. The steps share
// the record; a step that ends the pass records the result the controller
// returns.
type servicePass struct {
	reconciler *InferenceServiceReconciler
	isvc       *v1beta1.InferenceService
	log        logr.Logger

	// result is what the pass returns when a step ends it.
	result ctrl.Result
	// requeueNoLaterThan caps the requeue so the next pass runs before
	// spec.ttlSecondsAfterCreation passes; zero when no TTL applies.
	requeueNoLaterThan time.Duration

	// policy is the execution policy placement wrote on a member copy, and
	// placementMember is true once the hub has granted one; until then the
	// service is the local service it still is.
	policy          *v1beta1.PlacementExecutionPolicy
	placementMember bool
	deployConfig    *controllerconfig.DeployConfig
	// deploymentMode is the service-level mode, which decides the
	// VirtualDeployment pass and the status; each Component's own mode is in
	// resolved.Modes once its spec is merged.
	deploymentMode constants.DeploymentModeType
	// base is the canary records as this pass read them, so the status flush
	// keeps a newer live record over a decision made from a stale copy.
	base       *canary.Base
	isvcConfig *controllerconfig.InferenceServicesConfig
	// modelConfig writes the mounted model configuration; a placement
	// member's waits for the member rules.
	modelConfig    *multimodelconfig.ConfigMapReconciler
	policyResolver *autoscaler.PolicyResolver

	// in is what resolution reads, and res what it produced: the model, the
	// overlays, the runtime, the three merged Component specs and their
	// deployment modes.
	in       render.Inputs
	resolved *render.Resolved
	// componentRunnerPorts is the port each Component serves on; the
	// per-revision Services of the rollout layers publish it.
	componentRunnerPorts map[v1beta1.ComponentType][]v1.ContainerPort
	// componentServicePortAppProtocols is the appProtocol each Component
	// declares per Service port name. The per-revision Services of the
	// rollout layers publish it the way the stable Service does.
	componentServicePortAppProtocols map[v1beta1.ComponentType]map[string]string

	scalingPolicy   *v1beta1.ScalingPolicy
	scalingSource   string
	componentBounds map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec
	// scalingTargets is the count the coordinator resolved for each follower
	// this pass; each follower's projection writes it beside its partition.
	scalingTargets *scaling.Targets

	// runOutcome is what the run layer decided; nil when the service neither
	// declares nor carries a rollout.
	runOutcome      *rolloutrun.Outcome
	rolloutConfig   *controllerconfig.RolloutConfig
	metricProviders controllerconfig.MetricProvidersConfig
	// policies is the RolloutPolicy set as the run layer observed it; every
	// reader of the effective plan resolves through the same set.
	policies rollout.Policies

	reconcilers           []components.Component
	ingressDeploymentMode constants.DeploymentModeType
	// pendingRequeue gathers the requeue the run layer, the components and
	// the canary groups asked for, folded by SoonestRequeue: any Requeue,
	// the soonest positive RequeueAfter.
	pendingRequeue reconcile.Result
	// canaryConsume lists the operator annotations the canary passes
	// applied; they are removed once the status write that carries their
	// effect has landed.
	canaryConsume []string
	// resolvedIngressConfig is the ingress config with the service's
	// annotation overrides applied; set by the route render.
	resolvedIngressConfig *controllerconfig.IngressConfig
	// hadTrafficTargets says whether the service entered the pass with
	// traffic targets published; with the state after the flush, it decides
	// whether the routes are rendered before a requeue.
	hadTrafficTargets bool
	// componentWriteErr is the first write a component reconciler could not
	// make this pass. The pass still runs the steps that only read the
	// world (the coordination pass, the status flush with its route render),
	// skips the steps that build on this pass's projections, and returns it.
	componentWriteErr error
}

// passStep is one step of a pass. It reports whether the pass goes on; a
// step that ends the pass has recorded the result on the pass first.
type passStep func(context.Context) (bool, error)

// run takes the pass through its steps in order and stops at the first one
// that ends it. The list is the whole order of a pass. The result the pass
// recorded, capped by the TTL, is what the controller returns.
func (p *servicePass) run(ctx context.Context) (ctrl.Result, error) {
	_, err := runPassSteps(ctx, []passStep{
		p.deleteWhenExpired,
		p.checkPlacementIntent,
		p.resolveDeploymentMode,
		p.addFinalizer,
		p.finishDeletion,
		p.serveVirtualDeployment,
		p.prepareStatus,
		p.reportCRDSchema,
		p.loadServiceConfig,
		p.frontReferencedReplicas,
		p.reconcileModelConfig,
		p.buildPolicyResolver,
		p.resolveModel,
		p.resolveOverlays,
		p.useRecordedRuntimeRevision,
		p.resolveRuntime,
		p.prepareComponentSpecs,
		p.checkEncoderModes,
		p.resolveScalingPolicy,
		p.checkPlacementMember,
		p.reconcileRolloutRun,
		p.bindCanaryToOpenedRun,
		p.flushRunBoundary,
		p.resolveScalingTargets,
		p.buildComponentReconcilers,
		p.chooseIngressMode,
		p.runComponentReconcilers,
		p.unlessAComponentWriteFailed(p.removeReplacedBackendObjects),
		p.unlessAComponentWriteFailed(p.runScalingCoordinator),
		p.unlessAComponentWriteFailed(p.dispatchCanaryGroups),
		p.unlessAComponentWriteFailed(p.releaseUndeclaredUnits),
		p.reconcileCoordination,
		p.flushRequeueingPass,
		p.reconcileIngress,
		p.reconcileExternalService,
		p.reconcileTraffic,
		p.setURLWhenIngressDisabled,
		p.cleanupUndeclaredComponents,
		p.flushSettledPass,
	})
	if err == nil && p.componentWriteErr != nil {
		// The refused write is the pass's result: the controller retries it
		// with backoff, with the traffic and the status of this pass landed.
		return ctrl.Result{}, p.componentWriteErr
	}
	return requeueBy(p.result, err, p.requeueNoLaterThan), err
}

// unlessAComponentWriteFailed runs step only on a pass in which every
// component write landed; step builds on the projections of this pass.
func (p *servicePass) unlessAComponentWriteFailed(step passStep) passStep {
	return func(ctx context.Context) (bool, error) {
		if p.componentWriteErr != nil {
			return true, nil
		}
		return step(ctx)
	}
}

// runPassSteps runs the steps in order and reports whether every one let
// the pass go on. It stops at the first step that ends the pass or fails
// and returns that step's error.
func runPassSteps(ctx context.Context, steps []passStep) (bool, error) {
	for _, step := range steps {
		if next, err := step(ctx); !next || err != nil {
			return false, err
		}
	}
	return true, nil
}

// end records the result a step ends the pass with and reports that the
// pass does not go on.
func (p *servicePass) end(result ctrl.Result, err error) (bool, error) {
	p.result = result
	return false, err
}

// SoonestRequeue folds one more request to be looked at again into the
// pass's pending requeue. The run layer, every component reconciler and
// every canary group may ask; the pass comes back at the soonest of their
// asks, so a part that asked for a quick second look never waits on a part
// that asked for a slow one. A RequeueAfter of zero asks for nothing and
// never wins. A bare Requeue stays the flag it is, with its controller
// meaning: a timed requeue runs when one is set, else the rate-limited one.
func SoonestRequeue(pending, asked reconcile.Result) reconcile.Result {
	pending.Requeue = pending.Requeue || asked.Requeue
	if asked.RequeueAfter > 0 && (pending.RequeueAfter <= 0 || asked.RequeueAfter < pending.RequeueAfter) {
		pending.RequeueAfter = asked.RequeueAfter
	}
	return pending
}

// deleteWhenExpired deletes a service whose spec.ttlSecondsAfterCreation
// has passed, before anything that can fail, so an invalid spec cannot stop
// the deletion, and caps the requeue of one whose TTL has not. A placement
// member copy is left to the TTL of its source.
func (p *servicePass) deleteWhenExpired(ctx context.Context) (bool, error) {
	if !p.isvc.DeletionTimestamp.IsZero() || protocol.IsMember(p.isvc) {
		return true, nil
	}
	remaining, ok := ttlRemaining(p.isvc, p.reconciler.now())
	if !ok {
		return true, nil
	}
	if remaining <= 0 {
		p.log.Info("Deleting InferenceService because ttlSecondsAfterCreation has passed",
			"ttlSecondsAfterCreation", *p.isvc.Spec.TTLSecondsAfterCreation)
		return p.end(ctrl.Result{}, p.reconciler.deleteExpired(ctx, p.isvc))
	}
	p.requeueNoLaterThan = remaining
	return true, nil
}

// checkPlacementIntent ends the pass, before any write, on a malformed
// placement intent or on an execution policy that is malformed or belongs
// to another owner. A placement mark with no policy yet does not end it:
// the hub has not granted authority, so the service notes that and goes on
// as a local service. A service being deleted goes to its finalizer instead.
func (p *servicePass) checkPlacementIntent(ctx context.Context) (bool, error) {
	if !p.isvc.DeletionTimestamp.IsZero() {
		return true, nil
	}
	if err := validation.ValidatePlacementIntent(p.isvc); err != nil {
		return false, err
	}
	var err error
	if p.policy, err = protocol.FromDerived(p.isvc); err != nil {
		return false, err
	}
	if protocol.IsMember(p.isvc) && p.policy == nil {
		if result, err := p.reconciler.notePlacementExecutionPending(ctx, p.isvc); err != nil || !result.IsZero() {
			return p.end(result, err)
		}
	}
	return true, nil
}

// resolveDeploymentMode loads the deploy config and resolves the
// service-level mode. A placement member in VirtualDeployment is held: the
// member rules need OMENative's admission and surge accounting.
func (p *servicePass) resolveDeploymentMode(ctx context.Context) (bool, error) {
	deployConfig, err := controllerconfig.NewDeployConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset)
	if err != nil {
		return false, errors.Wrapf(err, "fails to create DeployConfig")
	}
	p.deployConfig = deployConfig
	p.deploymentMode = isvcutils.InferenceServiceDeploymentMode(p.isvc, constants.DeploymentModeType(deployConfig.DefaultDeploymentMode))
	p.log.V(1).Info("InferenceService deployment mode resolved", "deploymentMode", p.deploymentMode)
	p.placementMember = protocol.IsMember(p.isvc) && p.policy != nil
	if p.placementMember && p.isvc.DeletionTimestamp.IsZero() && p.deploymentMode == constants.VirtualDeployment {
		return p.end(p.reconciler.holdPlacementBackend(ctx, p.isvc, fmt.Errorf("multicluster placement requires OMENative; VirtualDeployment has no member admission or surge accounting")))
	}
	return true, nil
}

// addFinalizer puts the controller's finalizer on a service that lacks it,
// so its deletion passes through finishDeletion. A conflict means another
// writer moved the object after this pass read it; the next pass adds the
// finalizer to a fresh copy.
func (p *servicePass) addFinalizer(ctx context.Context) (bool, error) {
	if !p.isvc.ObjectMeta.DeletionTimestamp.IsZero() || controllerutil.ContainsFinalizer(p.isvc, inferenceServiceFinalizer) {
		return true, nil
	}
	controllerutil.AddFinalizer(p.isvc, inferenceServiceFinalizer)
	if err := p.reconciler.Update(ctx, p.isvc); err != nil {
		if apierrors.IsNotFound(err) {
			return p.end(ctrl.Result{}, nil)
		}
		if apierrors.IsConflict(err) {
			return p.end(ctrl.Result{Requeue: true}, nil)
		}
		return false, err
	}
	return true, nil
}

// finishDeletion ends the pass of a service being deleted. While the
// finalizer is still on it, the pass drops the service's coordination,
// canary and readiness metric series, so the per-service vectors do not
// grow without bound, and removes the finalizer; a conflict on that write
// is retried by the next pass.
func (p *servicePass) finishDeletion(ctx context.Context) (bool, error) {
	if p.isvc.ObjectMeta.DeletionTimestamp.IsZero() {
		return true, nil
	}
	if controllerutil.ContainsFinalizer(p.isvc, inferenceServiceFinalizer) {
		coordination.DeleteForISVC(p.isvc.Namespace, p.isvc.Name)
		canary.DeleteForISVC(p.isvc.Namespace, p.isvc.Name)
		obsmetrics.DeleteISVCSeries(p.isvc.Namespace, p.isvc.Name)
		controllerutil.RemoveFinalizer(p.isvc, inferenceServiceFinalizer)
		if err := p.reconciler.Update(ctx, p.isvc); err != nil {
			if apierrors.IsNotFound(err) {
				return p.end(ctrl.Result{}, nil)
			}
			if apierrors.IsConflict(err) {
				return p.end(ctrl.Result{Requeue: true}, nil)
			}
			return false, err
		}
	}
	return p.end(ctrl.Result{}, nil)
}

// serveVirtualDeployment ends the pass of a VirtualDeployment, which selects
// no backend: whatever a backend projected before is removed and the status
// reports the virtual endpoint.
func (p *servicePass) serveVirtualDeployment(ctx context.Context) (bool, error) {
	if p.deploymentMode != constants.VirtualDeployment {
		return true, nil
	}
	return p.end(p.reconciler.handleVirtualDeployment(ctx, p.isvc))
}

// prepareStatus takes the canary records as this pass read them, so the
// status flush keeps a newer live record over a decision made from a stale
// copy, and gives the component status writers their map.
func (p *servicePass) prepareStatus(context.Context) (bool, error) {
	p.base = canary.NewBase(p.isvc)
	p.hadTrafficTargets = publishesTrafficTargets(p.isvc)
	if p.isvc.Status.Components == nil {
		p.isvc.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
	}
	return true, nil
}

// loadServiceConfig loads the inferenceservices block of the operator
// config, with the model cache provider's settings attached.
func (p *servicePass) loadServiceConfig(context.Context) (bool, error) {
	p.log.V(1).Info("Reconciling inference service")
	isvcConfig, err := controllerconfig.NewInferenceServicesConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset)
	if err != nil {
		return false, errors.Wrapf(err, "fails to create InferenceServicesConfig")
	}
	p.isvcConfig = isvcConfig
	return true, nil
}

// frontReferencedReplicas ends the pass of a service over referenced
// replicas, which renders and projects nothing: its pass is the fronting
// alone.
func (p *servicePass) frontReferencedReplicas(ctx context.Context) (bool, error) {
	if !irprojector.ReferencesReplicas(p.isvc) {
		return true, nil
	}
	return p.end(p.reconciler.reconcileReferencedReplicas(ctx, p.isvc, p.deploymentMode, p.isvcConfig, p.base))
}

// reconcileModelConfig writes the mounted model configuration of a local
// service. A placement member's waits for the member rules.
func (p *servicePass) reconcileModelConfig(ctx context.Context) (bool, error) {
	p.modelConfig = multimodelconfig.NewModelConfigReconciler(p.reconciler.Client, p.reconciler.Scheme, p.reconciler.Recorder)
	if p.placementMember {
		return true, nil
	}
	if result, err := p.modelConfig.Reconcile(ctx, p.isvc); err != nil {
		return p.end(result, err)
	}
	return true, nil
}

// buildPolicyResolver builds the AutoscalerPolicy resolver from the cached
// operator config, so a provider-binding edit applies within one config
// TTL. With the feature absent the resolver refuses every reference instead
// of ignoring it.
func (p *servicePass) buildPolicyResolver(context.Context) (bool, error) {
	p.policyResolver = &autoscaler.PolicyResolver{Enabled: false}
	if !p.reconciler.AutoscalerPolicyEnabled {
		return true, nil
	}
	policyConfig, err := controllerconfig.NewAutoscalerPolicyConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset)
	if err != nil {
		return false, errors.Wrapf(err, "fails to load autoscalerPolicy config")
	}
	providers, providerAuth := autoscaler.ProviderBindingsFromConfig(policyConfig)
	p.policyResolver = &autoscaler.PolicyResolver{
		Client:        p.reconciler.Client,
		Providers:     providers,
		ProviderAuth:  providerAuth,
		Enabled:       true,
		KedaAvailable: p.reconciler.KedaAvailable,
		// A hold caused by the operator config (an unbound provider) ends on
		// a config edit, which raises no event on the service, so the service
		// requeues once per config cache TTL. A TTL of zero or less means
		// live config reads and no periodic requeue.
		HoldRequeueAfter: p.reconciler.ConfigCacheTTL,
	}
	return true, nil
}

// resolveModel reads the model the service names, ahead of the runtime, so
// a failed model read creates no revision. A service with no spec.model
// names its runtime directly and skips the model read, the readiness wait,
// the overlays and the runtime-against-model check. A sharded model that is
// not ready yet requeues the pass.
func (p *servicePass) resolveModel(ctx context.Context) (bool, error) {
	p.in = render.Inputs{Service: p.isvc, Client: p.reconciler.Client, Runtimes: p.reconciler.RuntimeSelector, Accelerators: p.reconciler.AcceleratorClassSelector, Config: p.isvcConfig, Log: p.reconciler.Log}
	var err error
	p.in.Model, p.in.ModelMeta, err = render.ResolveModel(ctx, p.in)
	if err != nil {
		var notReady *render.ModelNotReadyError
		if errors.As(err, &notReady) {
			message := notReady.Message
			if message == "" {
				message = "sharded BaseModel is not ready"
			}
			p.reconciler.Log.Info("Waiting for sharded BaseModel to become ready",
				"model", p.isvc.Spec.Model.Name,
				"namespace", p.isvc.Namespace,
				"message", message)
			p.reconciler.Recorder.Event(p.isvc, v1.EventTypeNormal, "ModelNotReady", message)
			return p.end(reconcile.Result{RequeueAfter: 5 * time.Second}, nil)
		}
		p.reconciler.Log.Error(err, "Failed to reconcile base model", "Name", p.isvc.Name)
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "ModelReconcileError", err.Error())
		return false, err
	}
	p.in.ModelRead = true
	return true, nil
}

// resolveOverlays reads the overlays the service names and records them on
// the status. A missing, disabled or not-ready overlay is a skipped entry;
// only a client error ends the pass.
func (p *servicePass) resolveOverlays(ctx context.Context) (bool, error) {
	overlays, err := render.ResolveOverlays(ctx, p.in)
	if err != nil {
		p.reconciler.Log.Error(err, "Failed to resolve overlays", "isvc", p.isvc.Name)
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "OverlayResolveError", err.Error())
		return false, err
	}
	p.in.Overlays = overlays
	p.in.OverlaysRead = true
	p.isvc.Status.MountedOverlays = components.MountedOverlaySummary(p.in.Overlays)
	setOverlaysReadyCondition(p.isvc, p.in.Overlays)
	return true, nil
}

// useRecordedRuntimeRevision applies to a service whose spec.runtime.autoSync
// is false: its runtime spec comes from the ControllerRevision recorded for
// it, not from the live runtime. The helper creates the first revision,
// detects drift and answers the ome.io/runtime-sync request; when it has
// written the status itself the children are skipped and the pass ends.
func (p *servicePass) useRecordedRuntimeRevision(ctx context.Context) (bool, error) {
	ref := p.isvc.Spec.Runtime
	if ref == nil || ref.Name == "" || ref.AutoSync == nil || *ref.AutoSync {
		return true, nil
	}
	recorded, err := p.reconciler.resolvePinnedRuntime(ctx, p.isvc)
	if err != nil {
		p.reconciler.Log.Error(err, "Pin resolution failed", "runtime", ref.Name)
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "RuntimePinError", err.Error())
		return false, err
	}
	if recorded.skipChildren {
		return p.end(reconcile.Result{}, nil)
	}
	p.in.Runtime = recorded.spec
	p.in.RuntimeName = ref.Name
	p.in.RuntimeIsCluster = sourceKindFor(ref) == runtimerevision.KindClusterServingRuntime
	return true, nil
}

// resolveRuntime looks the runtime up by name, or selects one for the model,
// refuses a disabled one, and merges the runtime's block and the service's
// into the three Component specs. A runtime the operator named serves a
// model it does not declare; the mismatch is an advisory event. A failure is
// reported by reportResolveError, which holds a service with no runtime
// behind RuntimeReady=False without an error.
func (p *servicePass) resolveRuntime(ctx context.Context) (bool, error) {
	resolved, err := render.Resolve(ctx, p.in)
	if err != nil {
		return p.end(p.reconciler.reportResolveError(p.isvc, p.deploymentMode, p.in, err))
	}
	p.resolved = resolved
	if resolved.CompatibilityAdvisory != nil {
		p.reconciler.Log.Info("Runtime named explicitly; proceeding despite declared-format mismatch",
			"runtime", resolved.RuntimeName, "model", p.isvc.Spec.Model.Name, "details", resolved.CompatibilityAdvisory.Error())
		p.reconciler.Recorder.Eventf(p.isvc, v1.EventTypeWarning, "RuntimeCompatibilityAdvisory",
			"Runtime %s does not declare support for model %s (%v); proceeding because the runtime was named explicitly",
			resolved.RuntimeName, p.isvc.Spec.Model.Name, resolved.CompatibilityAdvisory)
	}
	if p.in.Runtime == nil {
		// RuntimeReady follows the live lookup; a recorded revision reports
		// through RuntimeDrifted instead.
		p.reconciler.clearRuntimeUnresolved(p.isvc)
	}
	if !resolved.UserSpecifiedRuntime {
		p.log.Info("Auto-selected runtime", "runtime", resolved.RuntimeName, "model", p.isvc.Spec.Model.Name)
	}
	p.componentRunnerPorts = isvcutils.MergedRunnerPorts(resolved.Specs)
	p.componentServicePortAppProtocols = isvcutils.MergedServicePortAppProtocols(resolved.Specs)
	return true, nil
}

// prepareComponentSpecs decides each Component's deployment mode from its
// merged spec and fills the operator's deploy defaults in place. Placement
// checks the same resolved specs the projectors read, replica minimums from
// the operator configuration included.
func (p *servicePass) prepareComponentSpecs(context.Context) (bool, error) {
	if err := render.Prepare(p.resolved, p.isvc.Spec.DeploymentMode, p.deployConfig); err != nil {
		p.reconciler.Log.Error(err, "Failed to determine deployment modes", "Name", p.isvc.Name)
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "DeploymentModeError", err.Error())
		return false, err
	}
	return true, nil
}

// checkEncoderModes holds a service that declares an encoder while any of
// its roles resolves to a mode other than OMENative once the runtime's pieces
// are merged in. Only OMENative pods learn the other roles' addresses.
// Admission reads the service's own blocks, so a mode that only the runtime
// sets is caught here. The hold changes nothing in the cluster: it reports
// EncoderReady=False and a Warning event, and the running pods stay.
func (p *servicePass) checkEncoderModes(context.Context) (bool, error) {
	if p.resolved.Specs.Encoder == nil {
		return true, nil
	}
	for _, view := range p.resolved.Specs.Declared() {
		mode := p.resolved.Modes[view.Type]
		if mode == constants.OMENative {
			continue
		}
		role, _ := v1beta1.RoleOf(view.Type)
		source := "the service"
		if p.resolved.RuntimeName != "" {
			source = fmt.Sprintf("the service and its runtime %q (%s)", p.resolved.RuntimeName, role.RuntimeField)
		}
		cause := fmt.Errorf("%s resolves to deploymentMode %q from %s; a service with an encoder runs every role as OMENative, the only mode that gives the roles each other's addresses. Set the %s: OMENative annotation on %s, or remove the mode the runtime sets (%s)",
			role.ServiceField, mode, source, constants.DeploymentMode, role.ServiceField, validation.ReasonEncoderRequiresOMENative)
		return p.end(p.reconciler.holdEncoderModes(p.isvc, p.deploymentMode, cause))
	}
	return true, nil
}

// resolveScalingPolicy picks the service's scaling policy, else the
// runtime's, and gathers each declared Component's replica bounds, which
// follows reads.
func (p *servicePass) resolveScalingPolicy(context.Context) (bool, error) {
	p.scalingPolicy, p.scalingSource = scaling.Resolve(p.isvc, p.resolved.Runtime)
	p.componentBounds = map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{}
	for _, view := range p.resolved.Specs.Declared() {
		p.componentBounds[view.Type] = view.Extension
	}
	return true, nil
}

// follows reports whether the scaling coordinator owns the Component's
// replica count.
func (p *servicePass) follows(component v1beta1.ComponentType) bool {
	return scaling.Follows(p.scalingPolicy, component, p.componentBounds, p.resolved.Modes)
}

// checkPlacementMember applies the member rules once the hub has granted an
// execution policy: every Component resolves to OMENative, the replica
// minimums the policy sets match the merged specs, and only then is the
// mounted model configuration written. A failed rule holds the service
// behind an advisory condition.
func (p *servicePass) checkPlacementMember(ctx context.Context) (bool, error) {
	if !p.placementMember {
		return true, nil
	}
	if err := protocol.ValidateNativeModes(p.resolved.Modes); err != nil {
		return p.end(p.reconciler.holdPlacementBackend(ctx, p.isvc, err))
	}
	if err := checkPlacementReplicaFloors(p.isvc, p.resolved.Specs.Engine, p.resolved.Specs.Decoder, p.resolved.Specs.Router); err != nil {
		return p.end(p.reconciler.holdPlacementCondition(ctx, p.isvc, v1beta1.PlacementReplicaFloorsReady, "ReplicaFloorsMismatch", err))
	}
	clearPlacementReplicaFloorsHold(p.isvc)
	clearPlacementBackendHold(p.isvc)
	if result, err := p.modelConfig.Reconcile(ctx, p.isvc); err != nil {
		return p.end(result, err)
	}
	return true, nil
}

// reconcileRolloutRun runs the run layer when the service declares or
// carries a rollout. It resolves the effective plan and records it before
// the partition resolvers and the dispatch engines read it, so every reader
// in this pass sees the same plan. Recording the plan does not depend on the
// RolloutPolicy feature; only resolving a policy reference does. The run
// layer's requeue joins the pass's.
func (p *servicePass) reconcileRolloutRun(ctx context.Context) (bool, error) {
	// A service with both an engine and a decoder is prefill/decode
	// disaggregated; the line is a steady-state breadcrumb.
	if p.resolved.Specs.Engine != nil && p.resolved.Specs.Decoder != nil {
		p.log.V(1).Info("PD-disaggregated deployment detected")
	}
	if p.isvc.Spec.Rollout == nil && p.isvc.Status.Rollout == nil {
		return true, nil
	}
	var err error
	if p.rolloutConfig, err = controllerconfig.NewRolloutConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset); err != nil {
		return false, errors.Wrapf(err, "fails to load rollout config")
	}
	if p.metricProviders, err = controllerconfig.NewMetricProvidersConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset); err != nil {
		return false, errors.Wrapf(err, "fails to load metricProviders config")
	}
	bound := make(map[string]struct{}, len(p.metricProviders))
	for name := range p.metricProviders {
		bound[name] = struct{}{}
	}
	// The route provider this service's traffic intent targets is the
	// ingress config with the service's own ingress annotations applied,
	// the same resolution the route render and the admission webhook use.
	routeConfig, err := controllerconfig.NewIngressConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset)
	if err != nil {
		return false, errors.Wrapf(err, "fails to create IngressConfig")
	}
	outcome, err := rolloutrun.Reconcile(ctx, rolloutrun.Inputs{
		Client:                   p.reconciler.Client,
		Reader:                   p.reconciler.APIReader,
		Recorder:                 p.reconciler.Recorder,
		ISVC:                     p.isvc,
		FeatureEnabled:           p.reconciler.RolloutPolicyEnabled,
		BoundProviders:           bound,
		RouteCapabilities:        isvcutils.ResolveIngressConfig(routeConfig, p.isvc.Annotations).RouteCapabilities(),
		ComponentDeploymentModes: p.resolved.Modes,
	})
	if err != nil {
		p.log.Error(err, "Failed to reconcile rollout run")
		return false, errors.Wrapf(err, "fails to reconcile rollout run")
	}
	p.runOutcome = &outcome
	p.policies = outcome.Policies
	p.pendingRequeue = SoonestRequeue(p.pendingRequeue, reconcile.Result{RequeueAfter: outcome.RequeueAfter})
	return true, nil
}

// bindCanaryToOpenedRun binds or resets each canary group's state to a run
// the run layer opened this pass, before the run boundary is written, so
// the active run and its step state become visible together. An adopted
// run keeps its in-flight step and gains the run's target.
func (p *servicePass) bindCanaryToOpenedRun(ctx context.Context) (bool, error) {
	if p.runOutcome == nil || !p.runOutcome.Opened {
		return true, nil
	}
	for _, group := range rollout.CanaryGroups(p.isvc, p.policies) {
		open := canary.RunOpen{Adopted: p.runOutcome.Adopted, Closed: p.runOutcome.Closed}
		if err := canary.BindRun(ctx, p.reconciler.Client, p.isvc, group, open); err != nil {
			p.log.Error(err, "Failed to bind canary state to the rollout run")
			return false, errors.Wrapf(err, "fails to bind canary state to the rollout run")
		}
	}
	return true, nil
}

// flushRunBoundary writes the status as soon as the run layer opened,
// closed or re-recorded a run. The recorded plan steers the update gates
// and the partition; a later step that failed would otherwise drop it from
// memory, reopen the run on the next pass and move the projected spec under
// the executors. The coordination step has not run yet, so the write keeps
// the live group entries. A pass that read a stale canary record decides
// nothing further: the flush kept the live record, and the next pass
// decides from it.
func (p *servicePass) flushRunBoundary(context.Context) (bool, error) {
	if p.runOutcome == nil || !p.runOutcome.StateChanged {
		return true, nil
	}
	if err := p.reconciler.FlushRunBoundary(p.isvc, p.deploymentMode, p.base); err != nil {
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return false, err
	}
	if p.base.Stale() {
		return p.end(reconcile.Result{Requeue: true}, nil)
	}
	return true, nil
}

// resolveScalingTargets resolves each follower's count under a proportional
// scaling policy before the projectors run and hands it to the follower's
// reconciler, so the count and the canary partition sized from it reach the
// InferenceReplica in one write. The coordinator publishes its status after
// the projectors (runScalingCoordinator) and writes no replica itself.
func (p *servicePass) resolveScalingTargets(ctx context.Context) (bool, error) {
	targets, err := scaling.ResolveTargets(ctx, p.reconciler.APIReader, p.isvc, p.scalingPolicy, p.scalingSource,
		p.componentBounds, p.resolved.Modes)
	if err != nil {
		return false, errors.Wrap(err, "resolve component scaling")
	}
	p.scalingTargets = targets
	return true, nil
}

// buildComponentReconcilers builds one reconciler per declared Component, in
// the order engine, decoder, encoder, router. Each takes its render piece (the
// accelerator class and the model format; the router has neither, so its
// piece cannot fail), the policy resolver, the canary's partition resolver
// for its role and whether the coordinator owns its count.
func (p *servicePass) buildComponentReconcilers(ctx context.Context) (bool, error) {
	componentDeps := p.reconciler.buildComponentDeps(p.isvcConfig)
	if engine := p.resolved.Specs.Engine; engine != nil {
		piece, _, err := p.resolved.Piece(ctx, p.in, v1beta1.EngineComponent)
		if err != nil {
			p.log.Error(err, "Failed to get accelerator class for engine component")
			p.reconciler.Recorder.Eventf(p.isvc, v1.EventTypeWarning, "AcceleratorClassError", "Failed to get accelerator class for engine: %v", err)
			return false, err
		}
		p.log.V(1).Info("Creating engine reconciler",
			"deploymentMode", piece.DeploymentMode,
			"acceleratorClass", piece.AcceleratorClassName)
		p.reconcilers = append(p.reconcilers, components.NewEngine(componentDeps, p.inputsFor(piece, v1beta1.EngineComponent), engine))
	}
	if decoder := p.resolved.Specs.Decoder; decoder != nil {
		piece, _, err := p.resolved.Piece(ctx, p.in, v1beta1.DecoderComponent)
		if err != nil {
			p.log.Error(err, "Failed to get accelerator class for decoder component")
			p.reconciler.Recorder.Eventf(p.isvc, v1.EventTypeWarning, "AcceleratorClassError", "Failed to get accelerator class for decoder: %v", err)
			return false, err
		}
		p.log.V(1).Info("Creating decoder reconciler",
			"deploymentMode", piece.DeploymentMode,
			"acceleratorClass", piece.AcceleratorClassName)
		p.reconcilers = append(p.reconcilers, components.NewDecoder(componentDeps, p.inputsFor(piece, v1beta1.DecoderComponent), decoder))
	}
	if encoder := p.resolved.Specs.Encoder; encoder != nil {
		piece, _, err := p.resolved.Piece(ctx, p.in, v1beta1.EncoderComponent)
		if err != nil {
			p.log.Error(err, "Failed to get accelerator class for encoder component")
			p.reconciler.Recorder.Eventf(p.isvc, v1.EventTypeWarning, "AcceleratorClassError", "Failed to get accelerator class for encoder: %v", err)
			return false, err
		}
		p.log.V(1).Info("Creating encoder reconciler",
			"deploymentMode", piece.DeploymentMode,
			"acceleratorClass", piece.AcceleratorClassName)
		p.reconcilers = append(p.reconcilers, components.NewEncoder(componentDeps, p.inputsFor(piece, v1beta1.EncoderComponent), encoder))
	}
	if router := p.resolved.Specs.Router; router != nil {
		piece, _, _ := p.resolved.Piece(ctx, p.in, v1beta1.RouterComponent)
		p.log.V(1).Info("Creating router reconciler", "deploymentMode", piece.DeploymentMode)
		p.reconcilers = append(p.reconcilers, components.NewRouter(componentDeps, p.inputsFor(piece, v1beta1.RouterComponent), router))
	}
	return true, nil
}

// inputsFor is one role's per-pass inputs: its render piece, the policy
// resolver, the canary's partition resolver, which sizes the current step's
// hold against the count the projector writes so the partition and the
// replica count describe the same fleet, the follower flag, the count the
// scaling coordinator resolved for the role, whether the role stopped
// following and keeps its count, and the stable revision the role's canary
// unit keeps a side on, so a replica created under a rollback, a step or a
// stop comes up on that revision.
func (p *servicePass) inputsFor(piece *render.Piece, component v1beta1.ComponentType) components.ComponentInputs {
	in := componentInputs(piece, p.policyResolver, canary.PartitionResolver(p.isvc, p.policies, component), p.follows(component),
		p.scalingTargets.Replicas(component), p.scalingTargets.Released(component))
	in.RollbackToRevision = canary.StableSideTarget(p.isvc, p.policies, component)
	return in
}

// chooseIngressMode takes the ingress deployment mode from the router, else
// the decoder, else the engine, from the modes already decided, so the
// ingress reconciler and the Components cannot disagree.
func (p *servicePass) chooseIngressMode(context.Context) (bool, error) {
	switch {
	case p.resolved.Specs.Router != nil:
		p.ingressDeploymentMode = p.resolved.Modes[v1beta1.RouterComponent]
	case p.resolved.Specs.Decoder != nil:
		p.ingressDeploymentMode = p.resolved.Modes[v1beta1.DecoderComponent]
	default:
		p.ingressDeploymentMode = p.resolved.Modes[v1beta1.EngineComponent]
	}
	p.log.V(1).Info("Determined ingress deployment mode", "ingressDeploymentMode", p.ingressDeploymentMode)
	return true, nil
}

// runComponentReconcilers runs the component reconcilers in order. The
// first error ends the loop and becomes the pass's result, but not the
// pass: the replica controllers drain and build pods on their own clock,
// so the per-revision Services, the traffic weights and the status must
// follow the pods on this pass as on any other. A failure is often a write
// the apiserver rejects, which only a change to the service fixes, so it is
// recorded as an event on the service; a conflict is retried at once and
// gets no event, and an error of a pass the manager's shutdown stopped ends
// the pass with neither event nor error log. A requeue request does not
// end the loop, because the rollout layers must run on every pass; the
// soonest request is kept.
func (p *servicePass) runComponentReconcilers(ctx context.Context) (bool, error) {
	for _, reconciler := range p.reconcilers {
		result, err := reconciler.Reconcile(ctx, p.isvc)
		if err != nil {
			if shutdownCutShort(ctx, err) {
				p.log.V(1).Info("Component reconcile stopped by the manager's shutdown",
					"component", fmt.Sprintf("%T", reconciler), "error", err)
				return p.end(result, err)
			}
			if !apierrors.IsConflict(err) {
				p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "ComponentReconcileError", err.Error())
			}
			p.log.Error(err, "Failed to reconcile component",
				"component", fmt.Sprintf("%T", reconciler))
			p.componentWriteErr = err
			p.pendingRequeue.Requeue = true
			return true, nil
		}
		p.pendingRequeue = SoonestRequeue(p.pendingRequeue, result)
	}
	return true, nil
}

// removeReplacedBackendObjects removes the objects of a backend a Component
// left. A Component runs under exactly one backend, the one its mode
// selects, and the two backends share the Component's Service, disruption
// budget, autoscaler and accelerator capacity, so the removal lands in the
// pass that projects the selected backend and before any requeue.
func (p *servicePass) removeReplacedBackendObjects(ctx context.Context) (bool, error) {
	err := p.reconciler.cleanupReplacedBackends(ctx, p.isvc, p.resolved.Modes)
	if err == nil {
		return true, nil
	}
	if shutdownCutShort(ctx, err) {
		p.log.V(1).Info("Replaced backend cleanup stopped by the manager's shutdown", "error", err)
		return false, err
	}
	p.log.Error(err, "Failed to remove the objects of a replaced backend")
	p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "DeploymentModeChangeError", err.Error())
	return false, err
}

// runScalingCoordinator writes status.scalingPolicy once the projectors have
// run: each follower's target beside the count its projection written, read
// back after the projectors ran.
func (p *servicePass) runScalingCoordinator(ctx context.Context) (bool, error) {
	if err := scaling.Reconcile(ctx, p.reconciler.APIReader, p.reconciler.Recorder, p.isvc, p.scalingTargets); err != nil {
		return false, errors.Wrap(err, "coordinate component scaling")
	}
	return true, nil
}

// dispatchCanaryGroups runs the canary step machine once per canary group.
// The groups split the Components into disjoint sets, so each run sees only
// its own step counter; each run's requeue joins the pass's, the soonest
// kept, so a group mid-step is not paced by a group that waits. The metrics
// source and the query timeout come from the canaryAnalysis config on every
// pass, so a ConfigMap edit applies without a restart; only analysis steps
// read them.
func (p *servicePass) dispatchCanaryGroups(ctx context.Context) (bool, error) {
	canaryGroups := rollout.CanaryGroups(p.isvc, p.policies)
	if len(canaryGroups) == 0 {
		return true, nil
	}
	analysisConfig, err := controllerconfig.NewCanaryAnalysisConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset)
	if err != nil {
		p.log.Error(err, "Failed to load canaryAnalysis config")
		return false, errors.Wrapf(err, "fails to load canaryAnalysis config")
	}
	for _, group := range canaryGroups {
		out, err := canary.Dispatch(ctx, p.canaryDispatchDeps(group, analysisConfig))
		if err != nil {
			p.log.Error(err, "Failed to reconcile canary rollout")
			return false, errors.Wrapf(err, "fails to reconcile canary")
		}
		p.pendingRequeue = SoonestRequeue(p.pendingRequeue, reconcile.Result{Requeue: out.Requeue, RequeueAfter: out.RequeueAfter})
		p.canaryConsume = append(p.canaryConsume, out.Consume...)
	}
	return true, nil
}

// canaryDispatchDeps is what one canary group's dispatch reads: the live
// reader with the configured row decoder, because the crash reading decodes
// per-Instance rows; the analysis source, timeout and providers; and the
// cadences of the rollout config when the run layer loaded one.
func (p *servicePass) canaryDispatchDeps(group *v1beta1.RolloutGroup, analysisConfig *controllerconfig.CanaryAnalysisConfig) canary.DispatchDeps {
	deps := canary.DispatchDeps{
		ISVC:                     p.isvc,
		Client:                   p.reconciler.Client,
		Reader:                   irstatus.NewReader(p.reconciler.APIReader, p.reconciler.InstanceStatusDecoder),
		Recorder:                 p.reconciler.Recorder,
		Sampler:                  p.reconciler.CanarySampler,
		BundledPrometheusAddress: analysisConfig.BundledPrometheusAddress,
		QueryTimeout:             analysisConfig.QueryTimeoutDuration(),
		MetricProviders:          p.metricProviders,
		DefaultProvider:          analysisConfig.DefaultProvider,
		ComponentRunnerPorts:     p.componentRunnerPorts,
		Policies:                 p.policies,
		Group:                    group,
		Base:                     p.base,
		// Same declarations as the stable Service, so both Services agree.
		ComponentServicePortAppProtocols: p.componentServicePortAppProtocols,
	}
	if p.rolloutConfig != nil {
		deps.DefaultReadyTimeout = p.rolloutConfig.DefaultReadyTimeout
		deps.Requeue = p.rolloutConfig.CanaryRequeue
		deps.ParkedRequeue = p.rolloutConfig.CanaryParkedRequeue
	}
	return deps
}

// releaseUndeclaredUnits drops the status a canary projected onto a unit
// no group of the effective plan names any more (its record and phase), so
// its Components follow their own spec. The coordination step writes their
// traffic split from the pods, as it does for every Component.
func (p *servicePass) releaseUndeclaredUnits(context.Context) (bool, error) {
	canary.ReleaseUndeclaredUnits(p.isvc, p.policies)
	return true, nil
}

// reconcileCoordination runs the group coordination once every component
// reconciler has finished: it reads the per-revision pod counts, keeps the
// per-revision Services, writes each Component's traffic entries, drives
// the per-group state machine and sets RolloutCoordinationReady. The
// traffic weight deadband and the ratio tolerance come from the
// coordination config on every pass; an absent block means a zero deadband.
func (p *servicePass) reconcileCoordination(ctx context.Context) (bool, error) {
	coordinationConfig, err := controllerconfig.NewCoordinationConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset)
	if err != nil {
		p.log.Error(err, "Failed to load coordination config")
		return false, errors.Wrapf(err, "fails to load coordination config")
	}
	if _, err := coordination.Reconcile(ctx, coordination.ReconcileInputs{
		ISVC:     p.isvc,
		Policies: p.policies,
		Client:   p.reconciler.Client,
		// The coordination gates decode per-Instance rows, so the live reader
		// carries the configured row decoder.
		Reader:                       irstatus.NewReader(p.reconciler.APIReader, p.reconciler.InstanceStatusDecoder),
		Recorder:                     p.reconciler.Recorder,
		TrafficWeightDeadbandPercent: coordinationConfig.TrafficWeightDeadbandPercent,
		DefaultRatioTolerancePercent: coordinationConfig.DefaultRatioTolerancePercent,
		ComponentDeploymentModes:     p.resolved.Modes,
		ComponentRunnerPorts:         p.componentRunnerPorts,
		// Same declarations as the stable Service, so both Services agree.
		ComponentServicePortAppProtocols: p.componentServicePortAppProtocols,
	}); err != nil {
		p.log.Error(err, "Failed to reconcile cross-Component coordination")
		return false, errors.Wrapf(err, "fails to reconcile coordination")
	}
	return true, nil
}

// flushRequeueingPass ends a pass that asked to requeue, which is the steady
// state of an open rollout. It copies each replica's summary onto the
// service and writes the status, so the Component conditions the rollout
// layers gate on and the coordination's traffic entries land on every pass;
// a projection that only ran on settled passes would stay stale while the
// held rollout kept requeueing. The external Service and traffic wait for a
// settled pass. A stale canary record adds an immediate requeue.
//
// A service whose Components publish traffic targets renders its routes on
// this path too, after the flush: a rollout moves its targets on requeueing
// passes, and the flush is what decides whether this pass's targets stand or
// the live record is kept, so the routes read status once that is settled.
// The conditions the render sets ride the next pass that reaches the tail.
func (p *servicePass) flushRequeueingPass(ctx context.Context) (bool, error) {
	requeueing := p.pendingRequeue.Requeue || p.pendingRequeue.RequeueAfter > 0
	if !requeueing {
		return true, nil
	}
	if err := irprojector.AggregateIRStatus(ctx, p.reconciler.Client, p.reconciler.APIReader, p.isvc, p.resolved.Modes); err != nil {
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return false, err
	}
	if err := p.reconciler.FlushStatusThenConsume(ctx, p.isvc, p.deploymentMode, p.canaryConsume, p.base); err != nil {
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return false, err
	}
	if p.base.Stale() {
		p.pendingRequeue.Requeue = true
	}
	if p.hadTrafficTargets || publishesTrafficTargets(p.isvc) {
		if _, err := p.reconcileIngress(ctx); err != nil {
			return false, err
		}
	}
	return p.end(p.pendingRequeue, nil)
}

// reconcileIngress renders the routes OME owns for the service from its
// status: the Components that can back a route, and the traffic targets each
// one's rollout engine publishes. It runs on a settled pass, once the
// Components have created their Services, and before a requeue when the
// service publishes traffic targets: it loads the ingress config, applies the
// service's annotation overrides and reconciles the route for the ingress mode.
func (p *servicePass) reconcileIngress(ctx context.Context) (bool, error) {
	ingressConfig, err := controllerconfig.NewIngressConfigCached(p.reconciler.ConfigCache, p.reconciler.Clientset)
	if err != nil {
		return false, errors.Wrapf(err, "fails to create IngressConfig")
	}
	p.resolvedIngressConfig = isvcutils.ResolveIngressConfig(ingressConfig, p.isvc.Annotations)
	ingressReconciler := ingress.NewIngressReconciler(p.reconciler.Client, p.reconciler.Clientset, p.reconciler.Scheme, p.resolvedIngressConfig, p.isvcConfig)
	p.log.V(1).Info("Reconciling ingress for inference service")
	if err := ingressReconciler.(*ingress.IngressReconciler).ReconcileWithDeploymentMode(ctx, p.isvc, p.ingressDeploymentMode); err != nil {
		return false, errors.Wrapf(err, "fails to reconcile ingress")
	}
	return true, nil
}

// reconcileExternalService keeps the Service named after the
// InferenceService, which is its stable endpoint while ingress creation is
// disabled.
func (p *servicePass) reconcileExternalService(ctx context.Context) (bool, error) {
	externalServiceReconciler := external_service.NewExternalServiceReconciler(p.reconciler.Client, p.reconciler.Clientset, p.reconciler.Scheme, p.resolvedIngressConfig)
	p.log.V(1).Info("Reconciling external service for inference service")
	if err := externalServiceReconciler.Reconcile(ctx, p.isvc); err != nil {
		return false, errors.Wrapf(err, "fails to reconcile external service")
	}
	return true, nil
}

// reconcileTraffic applies the backend policy the active translator emits
// for the service's traffic intent and records the traffic status, on a
// failure too, so the operator sees the reason without waiting for a later
// pass. A controller set up without traffic management skips it.
func (p *servicePass) reconcileTraffic(ctx context.Context) (bool, error) {
	if p.reconciler.TrafficReconciler == nil {
		return true, nil
	}
	targetRoutes := traffic.ComputeTargetHTTPRoutes(p.isvc, p.resolved.Specs.Decoder != nil, p.resolved.Specs.Router != nil)
	trafficStatus, err := p.reconciler.TrafficReconciler.Reconcile(ctx, p.isvc, targetRoutes)
	p.isvc.Status.Traffic = trafficStatus
	if err != nil {
		p.reconciler.Log.Error(err, "Failed to reconcile traffic policy",
			"namespace", p.isvc.Namespace, "inferenceService", p.isvc.Name,
			"translator", p.reconciler.TrafficReconciler.TranslatorName())
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "TrafficReconcileError", err.Error())
		return false, errors.Wrapf(err, "fails to reconcile traffic policy")
	}
	return true, nil
}

// setURLWhenIngressDisabled runs while ingress creation is disabled: it
// marks the service so the cleanup keeps the external Service, and sets the
// service URL from that Service. A Service the cache has not observed yet is
// not a failure: the Service is owned and watched, so its create event
// brings the next pass.
func (p *servicePass) setURLWhenIngressDisabled(ctx context.Context) (bool, error) {
	if !p.resolvedIngressConfig.DisableIngressCreation {
		return true, nil
	}
	if err := p.reconciler.ensureIngressDisableAnnotation(p.isvc); err != nil {
		return false, errors.Wrapf(err, "fails to add ingress disable annotation")
	}
	if err := p.reconciler.setExternalServiceURL(ctx, p.isvc, p.resolvedIngressConfig); err != nil {
		if !apierrors.IsNotFound(err) {
			p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "InternalError", err.Error())
			return false, errors.Wrapf(err, "fails to set external service URL")
		}
		p.log.V(1).Info("external service absent; leaving the URL unset for this pass",
			"service", p.isvc.Name)
	}
	return true, nil
}

// cleanupUndeclaredComponents removes the objects, the status entry and the
// ready condition of a Component the spec no longer declares, so the Component
// stops gating the service's Ready. A failed object removal does not end the
// pass, so the status flush still lands; it is logged as an error so a lasting
// permission failure is visible.
func (p *servicePass) cleanupUndeclaredComponents(ctx context.Context) (bool, error) {
	specs := p.resolved.Specs
	if err := p.reconciler.cleanupRemovedComponents(ctx, p.isvc, specs); err != nil {
		p.log.Error(err, "Failed to cleanup removed components")
	}
	p.log.V(1).Info("Cleaning up component status",
		"mergedEngine", specs.Engine != nil,
		"mergedDecoder", specs.Decoder != nil,
		"mergedRouter", specs.Router != nil,
		"statusComponents", len(p.isvc.Status.Components))
	for _, role := range v1beta1.Roles {
		if specs.View(role.Type) != nil {
			continue
		}
		// The engine's condition always gates Ready and stays.
		if role.Type != v1beta1.EngineComponent {
			p.isvc.Status.RemoveCondition(role.ReadyCondition)
		}
		if _, present := p.isvc.Status.Components[role.Type]; present {
			delete(p.isvc.Status.Components, role.Type)
			p.log.Info(fmt.Sprintf("Deleted %s from status", role.Type))
		}
	}
	return true, nil
}

// flushSettledPass ends a pass that asked for no requeue. It copies each
// OMENative replica's status onto the service (the replica controller is
// the only writer of that status; a Raw or MultiNode Component's status was
// written by its own reconciler) and writes the status. A status conflict
// and a stale canary record requeue at once instead of reporting an error.
func (p *servicePass) flushSettledPass(ctx context.Context) (bool, error) {
	if err := irprojector.AggregateIRStatus(ctx, p.reconciler.Client, p.reconciler.APIReader, p.isvc, p.resolved.Modes); err != nil {
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return false, err
	}
	if err := p.reconciler.FlushStatusThenConsume(ctx, p.isvc, p.deploymentMode, p.canaryConsume, p.base); err != nil {
		if apierrors.IsConflict(err) {
			return p.end(ctrl.Result{Requeue: true}, nil)
		}
		p.reconciler.Recorder.Event(p.isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return false, err
	}
	if p.base.Stale() {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	return p.end(ctrl.Result{}, nil)
}

// publishesTrafficTargets reports whether any Component of the service
// carries traffic targets on its status. The routes of such a service follow
// the targets, so they are rendered on every pass and not only on the tail.
func publishesTrafficTargets(isvc *v1beta1.InferenceService) bool {
	for _, cs := range isvc.Status.Components {
		if len(cs.Traffic) > 0 {
			return true
		}
	}
	return false
}

// componentInputs carries one role's render inputs, with the reconcile's
// policy resolver, the role's canary partition and the count the scaling
// coordinator resolved for it, into its component reconciler.
func componentInputs(p *render.Piece, policies *autoscaler.PolicyResolver, partition irprojector.PartitionResolver, follower bool, target *int32, released bool) components.ComponentInputs {
	return components.ComponentInputs{
		DeploymentMode:       p.DeploymentMode,
		BaseModel:            p.BaseModel,
		BaseModelMeta:        p.BaseModelMeta,
		Runtime:              p.Runtime,
		RuntimeName:          p.RuntimeName,
		ModelFormat:          p.SupportedModelFormat,
		AcceleratorClass:     p.AcceleratorClass,
		AcceleratorClassName: p.AcceleratorClassName,
		Overlays:             p.Overlays,
		PolicyResolver:       policies,
		CoordinatorFollower:  follower,
		CoordinatorTarget:    target,
		FollowerReleased:     released,
		PacingPartition:      partition,
	}
}

// reportResolveError maps a failed runtime resolution or spec merge onto the
// event, log line and status the InferenceService reports for it. The model
// and the overlays are read before render.Resolve runs, so the failure is in
// the runtime step or the merge. A named-but-missing runtime, or a model no
// runtime supports, is a permanent user-config state: it parks the service
// behind an advisory condition instead of an error requeue, and the runtime
// watch re-triggers reconcile when a matching runtime appears.
func (r *InferenceServiceReconciler) reportResolveError(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType, in render.Inputs, err error) (reconcile.Result, error) {
	var modelName string
	if isvc.Spec.Model != nil {
		modelName = isvc.Spec.Model.Name
	}
	explicit := isvc.Spec.Runtime != nil && isvc.Spec.Runtime.Name != ""
	// A named live runtime is validated against the model before it is
	// fetched; a pin and a service without a model skip that check.
	validated := explicit && in.Runtime == nil && in.Model != nil
	var merge *render.MergeError
	var disabled *runtimeselector.RuntimeDisabledError
	switch {
	case errors.As(err, &merge):
		r.Log.Error(err, "Failed to merge specs", "Name", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "MergeSpecsError", err.Error())
		return reconcile.Result{}, err
	case errors.Is(err, render.ErrNoTemplateSource):
		// No model, no runtime — webhook should have caught this; defensive
		// guard for direct client writes that bypass admission.
		err := fmt.Errorf("InferenceService must specify spec.runtime when spec.model is omitted")
		r.Log.Error(err, "Cannot reconcile", "Name", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "RuntimeSelectionError", err.Error())
		return reconcile.Result{}, err
	case validated && !runtimeselector.IsRuntimeNotFoundError(err):
		// Everything the validation reports, other than the advisory
		// mismatch and a missing runtime, is a hard error: a disabled
		// runtime or a malformed model genuinely cannot run, a sharded
		// model with no configured cache provider physically cannot load
		// (the webhook's modelRequiresCacheProvider guard is the same
		// sharded check), and a runtime whose inheritance cannot be
		// resolved has no spec to serve.
		r.Log.Error(err, "Runtime validation failed", "runtime", isvc.Spec.Runtime.Name, "model", modelName)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeValidationError",
			"Runtime %s does not support model %s: %v", isvc.Spec.Runtime.Name, modelName, err)
		return reconcile.Result{}, err
	case errors.As(err, &disabled):
		r.Log.Error(err, "Runtime validation failed", "runtime", disabled.RuntimeName)
		r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeValidationError",
			"Runtime %s failed validation: %v", disabled.RuntimeName, err)
		return reconcile.Result{}, err
	case explicit && runtimeselector.IsRuntimeNotFoundError(err):
		return r.markRuntimeUnresolved(isvc, deploymentMode, isvc.Spec.Runtime.Name, err)
	case explicit:
		r.Log.Error(err, "Failed to get runtime spec", "runtime", isvc.Spec.Runtime.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "RuntimeFetchError", err.Error())
		return reconcile.Result{}, err
	case runtimeselector.IsNoRuntimeFoundError(err) || runtimeselector.IsRuntimeNotFoundError(err):
		return r.markRuntimeUnresolved(isvc, deploymentMode, modelName, err)
	}
	r.Log.Error(err, "Failed to auto-select runtime", "model", modelName)
	r.Recorder.Eventf(isvc, v1.EventTypeWarning, "RuntimeSelectionError",
		"Failed to find runtime for model %s: %v", modelName, err)
	return reconcile.Result{}, err
}

func (r *InferenceServiceReconciler) handleVirtualDeployment(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	// A VirtualDeployment runs no workload: every object a backend projected
	// for this InferenceService is removed before its status is written.
	if err := r.cleanupVirtualDeployment(ctx, isvc); err != nil {
		r.Log.Error(err, "Failed to remove the objects of a VirtualDeployment", "Name", isvc.Name)
		r.Recorder.Event(isvc, v1.EventTypeWarning, "DeploymentModeChangeError", err.Error())
		return reconcile.Result{}, err
	}

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

	if err := r.updateStatus(isvc, constants.VirtualDeployment, nil, groupEntriesFromCache); err != nil {
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
	if err := r.updateStatus(isvc, deploymentMode, nil, groupEntriesFromCache); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// holdEncoderModes reports a service with an encoder whose roles do not all
// resolve to OMENative and changes nothing else: EncoderReady reads False
// naming the role, and Ready follows it. The status is persisted and the
// reconcile returns without an error; an edit of the service or its runtime
// starts the next pass.
func (r *InferenceServiceReconciler) holdEncoderModes(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType, cause error) (reconcile.Result, error) {
	r.Recorder.Event(isvc, v1.EventTypeWarning, validation.ReasonEncoderRequiresOMENative, cause.Error())
	isvc.Status.SetCondition(v1beta1.EncoderReady, &knapis.Condition{
		Type:    v1beta1.EncoderReady,
		Status:  v1.ConditionFalse,
		Reason:  validation.ReasonEncoderRequiresOMENative,
		Message: cause.Error(),
	})
	if err := r.updateStatus(isvc, deploymentMode, nil, groupEntriesFromCache); err != nil {
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

// FlushStatusThenConsume writes the status, then removes the operator
// annotations whose effect that write carried. The order is the contract: a
// verb is consumed only once the state it produced is durable, and each
// applied verb leaves a record in status that keeps a still-visible
// annotation inert, so a removal that fails here is retried by a later pass
// rather than re-applied. It is the pass's flush; the replay driver enters
// it with the pass's own base and verbs.
func (r *InferenceServiceReconciler) FlushStatusThenConsume(ctx context.Context, isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType, consume []string, base *canary.Base) error {
	if err := r.updateStatus(isvc, deploymentMode, base, groupEntriesComputed); err != nil {
		return err
	}
	// Decisions made from a stale record were not written, so the verbs they
	// took stay in place for the pass that decides from the current record.
	if len(consume) == 0 || base.Stale() {
		return nil
	}
	if err := canary.ConsumeAnnotations(ctx, r.Client, isvc, consume); err != nil {
		r.Log.V(1).Info("operator annotation removal after the status write failed; a later pass retries it",
			"InferenceService", isvc.Name, "annotations", consume, "error", err.Error())
	}
	return nil
}

// FlushRunBoundary writes the status at the run boundary: as soon as the
// run layer opened, closed or re-recorded a run, before the pass's
// coordination step. The group entries the pass carries at that point are
// its cached copy's, so the write keeps the live ones. The replay driver
// enters it at the same point of its pass.
func (r *InferenceServiceReconciler) FlushRunBoundary(isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType, base *canary.Base) error {
	return r.updateStatus(isvc, deploymentMode, base, groupEntriesFromCache)
}

// groupEntries says what a status write carries for the coordination group
// entries, status.rolloutCoordination and the RolloutCoordinationReady
// condition. The pass computes them in its coordination step. A write made
// before that step carries them as the pass's cached copy held them, which
// is never newer than the live object (the cache trails the previous pass's
// write, never leads it), so such a write keeps the live entries.
type groupEntries int

const (
	// groupEntriesComputed marks a write after the coordination step: the
	// entries are the pass's own.
	groupEntriesComputed groupEntries = iota
	// groupEntriesFromCache marks a write before the coordination step: the
	// live entries are kept and the pass's copy reads them from then on.
	groupEntriesFromCache
)

func (r *InferenceServiceReconciler) updateStatus(desiredService *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType, base *canary.Base, entries groupEntries) error {
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
		mergeStatusOverLive(desiredService, latest, base, entries)
		if err := r.Status().Update(context.TODO(), latest); err != nil {
			return err
		}
		base.Advance(&latest.Status)
		return nil
	}); err != nil {
		return r.recordStatusWriteFailure(desiredService, err)
	}
	obsmetrics.RecordStatusUpdate(obsmetrics.ControllerISVC, obsmetrics.ResultSuccess)
	r.recordReadinessChange(desiredService, wasReady, preWriteReady)
	return nil
}

// mergeStatusOverLive puts the pass's status onto the live object the
// flush is about to write, keeping what the pass does not own or has not
// computed: each Component's lifecycle, a newer run record, a newer canary
// record and, for a write made before the coordination step, the group
// entries.
func mergeStatusOverLive(desiredService, latest *v1beta1.InferenceService, base *canary.Base, entries groupEntries) {
	preserved := preserveLifecycleStatus(latest.Status.Components)
	// Snapshot the live run state before the overwrite: the rollout
	// run's pinned plan must never move BACKWARD under a writer that
	// reconciled from a stale cache (a rolled-back pin is unrecoverable
	// — its repin verb was one-shot). The reconciler doesn't own that
	// subtree any more than it owns Lifecycle when another pass's write
	// is newer.
	liveRollout := latest.Status.Rollout.DeepCopy()
	// The group entries and their readiness condition as the live object
	// holds them: a phase the pass computed again keeps the instants the
	// live entry records for it.
	liveEntries := coordination.SnapshotLiveGroupEntries(&latest.Status)
	livePreStepHold := preStepHoldOf(&latest.Status)
	// The canary executor's record is guarded like the run pin: a pass
	// that read a stale copy keeps the live record, phase and traffic,
	// re-synced into its working copy so nothing after this write
	// decides from the stale one.
	base.PreserveFresh(&desiredService.Status, &latest.Status)
	if entries == groupEntriesFromCache {
		coordination.KeepLiveEntries(&desiredService.Status, &latest.Status)
	}
	latest.Status = desiredService.Status
	mergeLifecycleStatus(&latest.Status, preserved)
	rolloutrun.PreserveNewerRun(&latest.Status, liveRollout, livePreStepHold)
	liveEntries.KeepPhaseTimes(&latest.Status)
}

// recordStatusWriteFailure counts a failed status write by its kind and
// says what the caller returns: nothing for a service that vanished, the
// conflict for a lost optimistic-lock race the next pass retries, and the
// error, logged and recorded as an event, for anything else.
func (r *InferenceServiceReconciler) recordStatusWriteFailure(desiredService *v1beta1.InferenceService, err error) error {
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

// recordReadinessChange fires the Ready and NotReady events and the
// time-to-ready sample after a status write that flipped readiness.
func (r *InferenceServiceReconciler) recordReadinessChange(desiredService *v1beta1.InferenceService, wasReady bool, preWriteReady *knapis.Condition) {
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
}

// preStepHoldOf reports whether any canary unit of the status holds its
// pre-step: the hold exists so a replaced plan cannot raise traffic before
// the operator sees it, and that is true whichever unit set it.
func preStepHoldOf(status *v1beta1.InferenceServiceStatus) bool {
	for _, cs := range status.Components {
		if cs.Canary != nil && cs.Canary.PreStepHold {
			return true
		}
	}
	return status.Canary != nil && status.Canary.PreStepHold
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

// mergeLifecycleStatus restores the live lifecycle onto the Components the
// pass still reports. An entry the pass removed, because the spec no longer
// declares that Component, is not recreated: a removed Component keeps no
// status.
func mergeLifecycleStatus(s *v1beta1.InferenceServiceStatus, preserved map[v1beta1.ComponentType]*v1beta1.LifecycleStatus) {
	for c, om := range preserved {
		cs, present := s.Components[c]
		if !present {
			continue
		}
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
	// scheduler-plugins PodGroup detection. Optional CRD; absence is a
	// degradation surface, not a hard fail: the PodGroup watch below is
	// registered only when the CRD is present. The InferenceReplica
	// controller runs its own detection for the gang-scheduling path.
	podGroupFound, err := utils.IsCrdAvailable(r.ClientConfig, schedulingv1alpha1.SchemeGroupVersion.String(), constants.PodGroupKind)
	if err != nil {
		return err
	}
	r.componentDeps = &components.ComponentDeps{
		Client:                    r.Client,
		Clientset:                 r.Clientset,
		APIReader:                 r.APIReader,
		Recorder:                  r.Recorder,
		Scheme:                    r.Scheme,
		QuotaAcceleratorResources: r.QuotaAcceleratorResources,
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
	if err := RegisterRuntimeIndexes(context.Background(), mgr.GetFieldIndexer()); err != nil {
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
		// without touching spec/generation or metadata), except a change to a
		// canary record, which any writer may make and only a pass can read
		// back. Still trigger on spec (generation) changes AND on
		// label/annotation changes — OME drives canary promote/rollback via
		// annotations that do NOT bump generation, so a blanket
		// GenerationChangedPredicate would break rollouts.
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
		// A standalone replica a service fronts through spec.replicaRefs
		// carries no owner reference to it; its events reach the service
		// through this mapping.
		Watches(
			&v1beta1.InferenceReplica{},
			handler.EnqueueRequestsFromMapFunc(r.servicesReferencingReplica),
		).
		// EndpointSlice events for OMENative headless Services enqueue
		// the ISVC the Service belongs to so drain checks (IsPodDrained /
		// IsPodInRotation) react to kube-proxy convergence without waiting
		// on the periodic requeue. The mapper rejects slices that don't
		// target an OMENative-managed Service.
		Watches(
			&discoveryv1.EndpointSlice{},
			handler.EnqueueRequestsFromMapFunc(r.endpointSliceToISVC),
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
		if err := RegisterAutoscalerPolicyIndex(context.TODO(), mgr.GetFieldIndexer()); err != nil {
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
		if err := RegisterRolloutPolicyIndex(context.TODO(), mgr.GetFieldIndexer()); err != nil {
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
// in SetupWithManager (see RegisterRuntimeIndexes).
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

// RegisterRuntimeIndexes installs the runtime fan-out indexes: the name of
// the runtime an InferenceService references, and the services whose runtime
// is not resolved. It runs before the cache starts, so isvcsReferencingRuntime
// resolves referencing services through the indexes instead of scanning every
// cached InferenceService.
func RegisterRuntimeIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &v1beta1.InferenceService{}, isvcRuntimeNameIndexField, isvcRuntimeNameIndexExtractor); err != nil {
		return err
	}
	return indexer.IndexField(ctx, &v1beta1.InferenceService{}, isvcRuntimeUnresolvedIndexField, isvcRuntimeUnresolvedIndexExtractor)
}

// isvcsReferencingRuntime returns reconcile requests for EVERY ISVC whose
// spec.runtime.name matches obj or any runtime inheriting from it
// (respecting namespaced-SR scope), regardless of autoSync. Both float and
// pinned consumers must be re-reconciled on a runtime change:
//
//   - autoSync=true (default, "float"): the reconcile re-renders the pod
//     spec from the LIVE runtime, so a runtime edit rolls the ISVC forward.
//     Fanning these out is the whole point — without it a runtime change
//     never triggers the float ISVC's reconcile, so it silently fails to
//     propagate until some unrelated event happens to wake the ISVC.
//     Event-filter predicates and scoped informers keep incidental
//     reconciles rare, so nothing else would deliver the edit.
//
//   - autoSync=false ("pinned"): the reconcile runs drift detection and
//     warns the operator that the runtime edit was rejected; it does not roll.
//
// The fan-out follows inherit-from downwards, not just direct references.
// A consuming ISVC renders the runtime's chain resolved end to end on every
// reconcile, so editing a shared profile changes what every ISVC behind a
// child runtime produces; matching only the edited runtime's own name leaves
// those ISVCs on the pre-edit spec with no event left to correct them.
//
// NOTE: fanning out float ISVCs means a runtime edit rolls them. For
// RawDeployment that is a safe native rolling update; for OMENative it
// rolls per the Component's updateStrategy — an OMENative ISVC on
// RecreatePod with no rollout budget is recreated wholesale.
func (r *InferenceServiceReconciler) isvcsReferencingRuntime(ctx context.Context, obj client.Object) []reconcile.Request {
	// Empty namespace means cluster-scoped.
	root := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}

	sources := []types.NamespacedName{root}
	descendants, err := runtimeinheritance.Descendants(ctx, r.Client, root)
	if err != nil {
		// Degrade to the direct consumers rather than dropping the event.
		r.Log.Error(err, "fan-out runtime event: resolve inheriting runtimes failed", "runtime", root.Name)
	}
	sources = append(sources, descendants...)

	seen := sets.New[types.NamespacedName]()
	var reqs []reconcile.Request
	enqueue := func(namespace, name string) {
		key := types.NamespacedName{Namespace: namespace, Name: name}
		if seen.Has(key) {
			return
		}
		seen.Insert(key)
		reqs = append(reqs, reconcile.Request{NamespacedName: key})
	}

	for _, source := range sources {
		// Narrow to ISVCs whose spec.runtime.name matches via the field index;
		// the in-memory kind/namespace-scope filter below still applies.
		var isvcs v1beta1.InferenceServiceList
		if err := r.List(ctx, &isvcs, client.MatchingFields{isvcRuntimeNameIndexField: source.Name}); err != nil {
			r.Log.Error(err, "fan-out runtime event: list InferenceServices failed", "runtime", source.Name)
			continue
		}
		for i := range isvcs.Items {
			isvc := &isvcs.Items[i]
			if isvc.Spec.Runtime == nil || isvc.Spec.Runtime.Name != source.Name {
				continue
			}
			// Namespaced SR only matches same-namespace ISVCs; a cluster-scoped
			// runtime (empty namespace) matches ISVCs in any namespace.
			if source.Namespace != "" && isvc.Namespace != source.Namespace {
				continue
			}
			enqueue(isvc.Namespace, isvc.Name)
		}
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
		if root.Namespace != "" && isvc.Namespace != root.Namespace {
			continue
		}
		enqueue(isvc.Namespace, isvc.Name)
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
		hasPreference := isvc.Spec.AcceleratorSelector != nil
		for _, role := range v1beta1.ServiceRoles(&isvc.Spec).Declared() {
			hasPreference = hasPreference || role.AcceleratorOverride != nil
		}
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
			Client:                    r.Client,
			Clientset:                 r.Clientset,
			APIReader:                 r.APIReader,
			Recorder:                  r.Recorder,
			Scheme:                    r.Scheme,
			QuotaAcceleratorResources: r.QuotaAcceleratorResources,
			Config:                    cfg,
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

// RegisterAutoscalerPolicyIndex installs the index that lists the
// InferenceServices referencing an AutoscalerPolicy by its name.
func RegisterAutoscalerPolicyIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &v1beta1.InferenceService{}, autoscalerPolicyRefIndexField, autoscalerPolicyRefIndexer)
}

// RegisterRolloutPolicyIndex installs the index that lists the
// InferenceServices whose rollout groups reference a RolloutPolicy by its
// name.
func RegisterRolloutPolicyIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &v1beta1.InferenceService{}, rolloutpolicycontroller.PolicyRefIndexField, rolloutpolicycontroller.PolicyRefIndexer)
}

func autoscalerPolicyRefIndexer(obj client.Object) []string {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	var names []string
	for _, componentType := range v1beta1.RoleTypes() {
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
