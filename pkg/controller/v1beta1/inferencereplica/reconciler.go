// Package inferencereplica implements the controller for the
// InferenceReplica CRD.
//
// A projected replica exists per (InferenceService, Component); a
// standalone replica is created directly. The IR controller owns the
// per-Instance pipeline (Create / Update / Restart / Migrate / Delete) by
// handing a workload.ReconcileInput to workload.Reconcile each pass, and
// creates and reconciles the per-Component headless Service. The
// InferenceService controller owns the per-revision Services, the PodMonitor
// and coordination; this controller only creates, when absent, the
// per-revision Services of peer revisions its own pods name.
//
// Writers: the InferenceService controller writes a projected replica's
// spec, and any client that RBAC allows may write a standalone
// replica's spec (see the validating webhook in
// pkg/webhook/admission/inferencereplica/). This controller is the only
// writer of InferenceReplica.status.
//
// The controller is enabled by default; the
// --enable-inferencereplica-controller manager flag lets operators
// flip it off if a regression lands.
package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	workloadpodgroup "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadservice "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/service"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils"
)

// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/scale,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferencereplicas/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels;clusterbasemodels;servingruntimes;clusterservingruntimes;finetunedweights,verbs=get;list;watch

// Reconciler drives each IR lifecycle through the workload pipeline.
type Reconciler struct {
	client.Client
	Log      logr.Logger
	Recorder record.EventRecorder

	// Clientset reads the inferenceservice-config ConfigMap for operator
	// lifecycle configuration. When nil, each resolver applies its documented
	// compatibility or fail-safe behavior.
	Clientset kubernetes.Interface

	// RuntimeSelector resolves a replica's runtimeRef and selects a runtime
	// for a modelRef without one. Nil when this controller renders no refs: a
	// replica with refs then reports RenderFailed.
	RuntimeSelector runtimeselector.Selector

	// ConfigCache memoizes the inferenceservice-config ConfigMap for a
	// short, flag-driven TTL so per-reconcile config loads share one
	// apiserver GET — the same controllerconfig.ConfigCache the ISVC
	// controller uses (its own instance; the cache type is the shared
	// implementation). Initialized in SetupWithManager.
	ConfigCache *controllerconfig.ConfigCache
	// ConfigCacheTTL is the TTL applied to ConfigCache. Flag-driven (no
	// in-code behavioral default — supplied by the manager's
	// --config-cache-ttl flag / chart value); a non-positive value
	// disables caching (always reads the apiserver).
	ConfigCacheTTL time.Duration
	// ScaleUpPodBatchSize is the startup-validated missing-Pod budget. It is
	// immutable for the manager process lifetime; nil preserves the unbounded
	// compatibility behavior when the field is absent.
	ScaleUpPodBatchSize *int32
	// ScaleDownPodBatchSize is the startup-validated active delete budget in
	// Pod-equivalent units, as a count or percentage of the current Component.
	// It is immutable for the manager process lifetime; nil is unbounded.
	ScaleDownPodBatchSize *intstr.IntOrString
	// ScaleDownRequeueInterval is the startup-validated poll cadence for an
	// active delete wave. Zero disables cadence polling; watches and configured
	// lifecycle deadlines still schedule progress.
	ScaleDownRequeueInterval time.Duration
	// RepairBatchSize is the startup-validated bound on the crash-loop repairs
	// opened per Component in one pass, in Instance units. It is immutable for
	// the manager process lifetime; nil preserves unbounded repair when the
	// field is absent.
	RepairBatchSize *int32

	// APIReader is the live (uncached) API reader. Used for the
	// correctness-critical reads in revision bookkeeping and immutable topology
	// safety where stale cache contents could cause a collision or split a live
	// gang across domains. Required: SetupWithManager defaults it to
	// mgr.GetAPIReader() and rejects a reconciler still missing it.
	APIReader client.Reader

	// InstanceStatusDecoder decodes the per-Instance representation of every
	// InferenceReplica this reconciler reads, under the operator-configured
	// ColumnarV2 row bound. The zero value carries no bound: DenseV1 objects
	// decode unchanged and any ColumnarV2 object fails closed. Every IR read
	// that inspects rows goes through liveReader or cachedReader so the bound
	// travels with the reader into the status-mutation closures.
	InstanceStatusDecoder irstatus.Decoder

	// InstanceStatusTarget is the operator-configured representation every
	// status write of this reconciler targets, DenseV1 or ColumnarV2, loaded
	// once at startup from the same omenativeStatus configuration as the
	// decoder bound. It has no default: SetupWithManager rejects an unset
	// value and the writer refuses every write until it is set.
	InstanceStatusTarget irstatus.Encoding

	// Expectations is the create/delete bookkeeping cache the
	// workload pipeline uses to avoid re-issuing batches before the
	// controller-runtime watch has confirmed prior writes. Optional;
	// when nil the workload-level DefaultExpectations singleton is
	// used.
	Expectations *workloadtypes.Expectations

	// GangSchedulingAvailable is the cached cluster-discovery boolean —
	// true when the scheduler-plugins PodGroup CRD is installed. Set once
	// in SetupWithManager (mirrors the ISVC controller's flag) and threaded
	// into DesiredSpec so EnsurePodGroups runs. Without it, IR-managed
	// multi-node renders the gang reference on pods but never creates the
	// PodGroup object, leaving the gang stuck Pending ("PodGroup not found").
	GangSchedulingAvailable bool

	// MaxConcurrentReconciles caps parallel IR reconciles (distinct objects
	// only — controller-runtime serializes per object key, so independent IRs
	// reconcile in parallel safely). Sourced from a flag/chart value; no in-code
	// default. Zero (unset) preserves controller-runtime's single-worker default.
	MaxConcurrentReconciles int

	// revisionHashMu guards revisionHashCache. The same IR key is never
	// reconciled concurrently by controller-runtime, but the cache is
	// shared across keys, so the map itself needs a lock if concurrency is
	// ever raised above the default of one.
	revisionHashMu sync.Mutex
	// revisionHashCache memoizes the revision (hash, raw) per IR so the
	// full-PodSpec json.Marshal + FNV in revision.HashWithWorkerTopologyAndPairing
	// is skipped when the hash inputs are provably unchanged. Generation covers
	// the spec-derived inputs; the entry also records the remaining ones: the
	// excluded annotation list (an annotation, so editing it does not bump
	// generation), the collision count and the scope UID (revisionScopeUID).
	// Keyed by the IR's NamespacedName (so the NotFound branch can evict
	// without the UID) with the IR UID folded into the entry so a
	// delete-and-recreate of the same name misses the stale entry.
	revisionHashCache map[types.NamespacedName]revisionHashEntry

	// scaleDownSeriesMu guards the identity needed to remove per-IR metric
	// series after the object has disappeared and its labels are unavailable.
	scaleDownSeriesMu    sync.Mutex
	scaleDownSeriesCache map[types.NamespacedName]scaleDownSeriesIdentity

	// TPUSliceProvisioning is loaded once at startup. nil provisions no TPU
	// slices.
	TPUSliceProvisioning *controllerconfig.TPUSliceProvisioningConfig
	// sliceReader is the cached slice reader. SetupWithManager sets it when
	// TPUSliceProvisioning is set and the Slice CRD is installed; nil
	// provisions no TPU slices.
	sliceReader client.Reader

	// Clock supplies wall-clock time to the workload lifecycle layer.
	// SetupWithManager defaults it to the real clock; tests may inject a fake.
	Clock clock.Clock
}

// revisionHashEntry is one memoized revision hash plus the inputs that
// invalidate it. raw is the canonical serialized payload stored in
// ControllerRevision.Data.Raw; it is treated as immutable once cached.
type revisionHashEntry struct {
	uid                    types.UID
	generation             int64
	excludedAnnotationKeys string
	collisionCount         int32
	scopeUID               types.UID
	hash                   string
	raw                    []byte
	// twin is the same templates rendered without their worker count, the
	// name a revision minted without a width carries; zero without a width.
	twin revision.WidthFreeTwin
}

type scaleDownSeriesIdentity struct {
	uid       types.UID
	namespace string
	isvc      string
	component string
}

// Reconcile reads the InferenceReplica once, rewrites a stored row shape
// other than the configured one, sends a deleted replica to its teardown,
// puts the teardown finalizer on a new one, and then runs one replicaPass
// over the decoded object. A replica that is gone has nothing to reconcile:
// the pods and ControllerRevisions it owned are garbage collected through
// their owner references, and its per-object caches are dropped.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("inferencereplica", req.NamespacedName)

	// The entry read decodes the stored per-Instance representation into
	// the plain list; a payload that cannot be decoded stops the pass here,
	// before any lifecycle effect.
	ir := &v1beta1.InferenceReplica{}
	source, readErr := irstatus.GetDecoded(ctx, r.cachedReader(), req.NamespacedName, ir)
	if readErr != nil {
		if apierrors.IsNotFound(readErr) {
			r.forgetRevisionHash(req.NamespacedName)
			r.deleteRememberedScaleDownSeries(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		log.Error(readErr, "Failed to get InferenceReplica")
		return ctrl.Result{}, r.instanceStatusDecodeError(ir, readErr)
	}
	r.rememberScaleDownSeries(ir)

	// An object stored in a representation other than the configured target
	// is rewritten before any pass, the teardown included, mutates it. An
	// object already in the target representation costs no extra
	// serialization.
	if handled, result, err := r.reconcileStoredRepresentation(ctx, log, ir, source); handled {
		return result, err
	}

	// Every log line of the pass, and every callback that takes its logger
	// from ctx, carries the component, the parent and the generation.
	log = log.WithValues(
		"component", ir.Spec.Component,
		"parent", ir.ParentName(),
		"generation", ir.Generation)
	ctx = ctrl.LoggerInto(ctx, log)
	log.V(1).Info("Reconcile entry",
		"currentRevision", ir.Status.CurrentRevision,
		"updateRevision", ir.Status.UpdateRevision,
		"replicas", ir.Status.Replicas,
		"readyReplicas", ir.Status.ReadyReplicas)

	// A replica being deleted is reconciled as a desired count of zero, so
	// its pods drain and escalate through the scale-down pipeline before the
	// finalizer comes off; whatever remains is garbage collected through the
	// children's owner references.
	if ir.DeletionTimestamp != nil {
		return r.reconcileTeardown(ctx, log, ir)
	}
	if err := validatePlacementReplicaLimit(ir); err != nil {
		return ctrl.Result{}, err
	}

	// The teardown finalizer goes on before any other work, so a later
	// delete runs through the teardown. The validating webhook admits a
	// finalizer-only update without the controller-write annotation.
	if !controllerutil.ContainsFinalizer(ir, TeardownFinalizer) {
		controllerutil.AddFinalizer(ir, TeardownFinalizer)
		if err := r.Update(ctx, ir); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			// An add the apiserver refuses because the object is already
			// Terminating is the cache lagging a delete: the pass requeues
			// onto the teardown without an error or its backoff.
			fresh := &v1beta1.InferenceReplica{}
			switch gerr := r.APIReader.Get(ctx, req.NamespacedName, fresh); {
			case gerr == nil && fresh.DeletionTimestamp != nil:
				log.V(1).Info("Finalizer add rejected: IR already Terminating; requeueing onto the teardown path")
				return ctrl.Result{Requeue: true}, nil
			case gerr != nil && apierrors.IsNotFound(gerr):
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		// The update response carries the stored row shape; the pass keeps
		// reading the plain list.
		if _, err := r.InstanceStatusDecoder.Decode(ir); err != nil {
			return ctrl.Result{}, r.instanceStatusDecodeError(ir, err)
		}
	}

	// What the status said before the pass wrote anything, so a promotion,
	// a cleared current revision and a first Failed Instance are each
	// logged once, when they happen.
	priorAnyFailed := hasFailedInstance(ir.Status.InstanceStatuses)
	pass := &replicaPass{
		reconciler:           r,
		ir:                   ir,
		log:                  log,
		priorCurrentRevision: ir.Status.CurrentRevision,
		priorClearedRevision: withdrawnRevisionFor(&ir.Status),
	}
	result, err := pass.run(ctx)
	if pass.published && !priorAnyFailed && hasFailedInstance(ir.Status.InstanceStatuses) {
		log.Info("Instance escalation observed; at least one Instance reached Phase=Failed")
	}
	return result, err
}

// replicaPass is one reconcile pass over one InferenceReplica: what the pass
// read at its start and what its steps decide as they run. The steps share
// the record; a step that ends the pass records the result the controller
// returns.
type replicaPass struct {
	reconciler *Reconciler
	ir         *v1beta1.InferenceReplica
	log        logr.Logger

	// result is what the pass returns when a step ends it. err is the error
	// the engine step records; the steps after it keep an earlier error over
	// their own, so a slice or Service failure never hides the engine's.
	result ctrl.Result
	err    error

	// The revision pair as the status read it before the pass:
	// priorClearedRevision names the revision the current revision was
	// cleared from, empty when it was not cleared. The publication logs a
	// restored, promoted or cleared current revision against them.
	priorCurrentRevision string
	priorClearedRevision string
	// published is set once the publication has written the status, or
	// tried to, and logged the revision transitions. A pass that ended
	// before the publication, or whose publication stopped on a stale read,
	// leaves it false.
	published bool

	// parent is the InferenceService the events go to; nil for a standalone
	// replica and while the parent is gone.
	parent *v1beta1.InferenceService
	// The operator's lifecycle settings and policies for this pass. An
	// absent or invalid value turns its window or bound off; nothing in the
	// code supplies a default.
	settings          lifecycleSettings
	retryPolicy       *workloadtypes.RetryPolicy
	autoMigrateBudget int32
	forceDeletePolicy *workloadtypes.ForceDeletePolicy
	coordDefaults     coordination.GroupDefaults
	// runners is the pod template set of this pass: the stored runners, or
	// the ones rendered from the model and runtime references.
	runners []v1beta1.Runner

	// input is what the engine reads; the steps fill its observed state in.
	input workloadtypes.ReconcileInput
	// holdObserved tells an update pass that ran and found nothing to hold
	// from one that never ran this pass; for the second the status writer
	// falls back to the stored retry record.
	holdObserved bool
	hold         *workloadtypes.RolloutHold
	// instanceReadyTimeout is the readiness window of this Component's
	// operations: the spec's own, else the operator's. Zero opens operations
	// with no deadline.
	instanceReadyTimeout time.Duration

	// specTarget is the ControllerRevision of the rendered spec, always
	// reported as updateRevision. rollTarget is the revision the Instances
	// roll toward: the spec target, or rollbackRevision when the spec names
	// a revision this replica controls to roll back to.
	specTarget       *appsv1.ControllerRevision
	rollTarget       *appsv1.ControllerRevision
	rollbackRevision *appsv1.ControllerRevision
	plan             workloadtypes.ComponentPlan
	// publishOnReturn is set once the pass may write status: from then on
	// every return, early or not, publishes the status.
	publishOnReturn bool
	// instancesAtStart lists the Instances that had a row when the
	// publication was set up, before the engine ran: the publication carries
	// a hold recorded by an earlier pass only while this pass removed none
	// of them.
	instancesAtStart []int32

	peerRevisionFor   coordination.PeerRevisionFunc
	deps              workloadtypes.Deps
	podGroupInventory *workloadgang.PodGroupInventory
	slices            *slicePass
}

// passStep is one step of a pass. It reports whether the pass goes on; a
// step that ends the pass has recorded the result on the pass first.
type passStep func(context.Context) (bool, error)

// run takes the pass through its steps in order and stops at the first one
// that ends it. The list is the whole order of a pass. Once
// installStatusPublication has run, every return publishes the status,
// which may replace the result.
func (p *replicaPass) run(ctx context.Context) (result ctrl.Result, err error) {
	defer func() {
		if p.publishOnReturn {
			result, err = p.publishStatus(ctx, result, err)
		}
	}()
	_, err = runPassSteps(ctx, []passStep{
		p.resolveParent,
		p.loadLifecycleSettings,
		p.releaseHeldRetryRecord,
		p.resetFailedInstances,
		p.resolveOperatorPolicies,
		p.renderPodTemplates,
		p.buildEngineInput,
		p.readNodeExclusions,
		p.readCordonedInstances,
		p.resolveInstanceReadyTimeout,
		p.syncMigrationRecords,
		p.acceptMigrationRequests,
		p.holdDeadlinesWhilePaused,
		p.ensureSpecRevision,
		p.resolveRollbackTarget,
		p.wireCoordinationGates,
		p.readRevisionWidths,
		p.buildPlan,
		p.clearIdleScaleDownGauges,
		p.installStatusPublication,
		p.logRollbackTarget,
		p.snapshotLivePodsForScaleDown,
		p.reportReadyTimeout,
		p.recordUpdateRevision,
		p.resolvePeerRevisions,
		p.buildEngineDeps,
		p.observePodGroups,
		p.ensurePodGroups,
		p.provisionSlices,
		p.runEngine,
		p.tendSlices,
		p.reconcileHeadlessService,
	})
	return p.result, err
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
// pass does not go on. A step that fails without ending the pass leaves
// the result empty.
func (p *replicaPass) end(result ctrl.Result, err error) (bool, error) {
	p.result = result
	return false, err
}

// resolveParent reads the parent InferenceService, which the events go to.
// A replica can outlive its parent for a short time during a foreground
// deletion, so a parent that is not found or not readable is no parent, not
// a failed pass.
func (p *replicaPass) resolveParent(ctx context.Context) (bool, error) {
	p.parent = p.reconciler.resolveParent(ctx, p.ir)
	return true, nil
}

// loadLifecycleSettings reads the operator's lifecycle settings once for
// the pass. A zero field turns its window or bound off: a pod that stays
// unready is left to the readiness deadline, a pod that cannot be placed
// has its clock stopped instead of being failed, and a pass with work left
// requeues on the rate-limited backoff.
func (p *replicaPass) loadLifecycleSettings(context.Context) (bool, error) {
	p.settings = p.reconciler.resolveLifecycleSettings(p.log)
	return true, nil
}

// releaseHeldRetryRecord answers the operator's request to release a Held
// retry record, before the engine input is built, so this pass's observed
// state already lacks the released record. A request that was answered
// requeues.
func (p *replicaPass) releaseHeldRetryRecord(ctx context.Context) (bool, error) {
	requeue, err := p.reconciler.consumeReleaseHeldRequest(ctx, p.log, p.ir, p.parent, p.settings.RetryBlockHistoryLimit)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: consume release-held request: %w", err)
	}
	if requeue {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	return true, nil
}

// resetFailedInstances answers the operator's request to rebuild the Failed
// Instances, before the engine input is built, so this pass's observed
// state already shows the cleared operations and the create pass can
// rebuild. A request that was answered requeues.
func (p *replicaPass) resetFailedInstances(ctx context.Context) (bool, error) {
	requeue, err := p.reconciler.consumeResetInstancesRequest(ctx, p.log, p.ir, p.parent)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: consume reset-instances request: %w", err)
	}
	if requeue {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	return true, nil
}

// resolveOperatorPolicies reads the rest of the operator's policies for the
// pass: the same-target update retry policy (nil holds on the first
// failure), the relocation budget of a failed Instance (zero turns the
// relocation branch off), the force-delete policy for pods that stay
// Terminating (nil leaves them alone), and the coordination group defaults
// the update gate resolves groups with, as the service controller does.
func (p *replicaPass) resolveOperatorPolicies(context.Context) (bool, error) {
	p.retryPolicy = p.reconciler.resolveUpdateRetryPolicy(p.log)
	p.autoMigrateBudget = p.reconciler.resolveAutoMigrateBudget(p.log)
	p.forceDeletePolicy = p.reconciler.resolveForceDeletePolicy(p.log)
	p.coordDefaults = p.reconciler.resolveCoordinationGroupDefaults(p.log)
	return true, nil
}

// renderPodTemplates takes the pod templates of the pass: the stored
// runners, or, for a replica that names a model or a runtime, the runners
// rendered from them for this pass only; the stored spec never gains
// runners. A template source that cannot be rendered is published on the
// Ready condition and ends the pass; the runtime and model watches bring
// the replica back when the source changes.
func (p *replicaPass) renderPodTemplates(ctx context.Context) (bool, error) {
	p.runners = p.ir.Spec.Runners
	rendered, err := p.reconciler.renderFromRefs(ctx, p.ir)
	if err != nil {
		var blocked *renderBlocked
		if errors.As(err, &blocked) {
			return p.end(p.reconciler.reportRenderBlocked(ctx, p.ir, blocked))
		}
		return false, fmt.Errorf("InferenceReplica reconciler: render from refs (ir=%s/%s): %w", p.ir.Namespace, p.ir.Name, err)
	}
	if rendered != nil {
		p.runners = rendered
	}
	return true, nil
}

// buildEngineInput projects the replica onto the engine's input, with the
// pod batch sizes of the manager and the recorder of the update pass's
// hold. The recorder tells an update pass that ran and found nothing to
// hold from one that never ran, which the status writer reads differently.
func (p *replicaPass) buildEngineInput(ctx context.Context) (bool, error) {
	p.input = p.reconciler.buildReconcileInput(ctx, p.ir, p.runners, p.parent, p.retryPolicy, p.forceDeletePolicy, p.settings, p.autoMigrateBudget, p.coordDefaults)
	p.input.ScaleUpPodBatchSize = p.reconciler.ScaleUpPodBatchSize
	p.input.ScaleDownPodBatchSize = p.reconciler.ScaleDownPodBatchSize
	p.input.ScaleDownRequeueInterval = p.reconciler.ScaleDownRequeueInterval
	p.input.RepairBatchSize = p.reconciler.RepairBatchSize
	p.input.RecordRolloutHold = func(hold *workloadtypes.RolloutHold) {
		p.holdObserved = true
		p.hold = hold
	}
	return true, nil
}

// readNodeExclusions removes the relocation records of Instances observed
// Ready and projects the per-Instance node exclusions the renderer turns
// into NotIn terms. An unreadable record set drops the exclusions for this
// pass only: one rebuild may then land on the suspect node again.
func (p *replicaPass) readNodeExclusions(ctx context.Context) (bool, error) {
	p.input.ObservedState.ExcludedNodesByInstance = p.reconciler.reconcileRelocationDirectives(ctx, p.log, p.ir, p.parent, p.autoMigrateBudget)
	return true, nil
}

// readCordonedInstances finds the Instances with a pod on a cordoned node,
// which a scale-down removes before any other. An unreadable cordon drops
// the preference for this pass only.
func (p *replicaPass) readCordonedInstances(ctx context.Context) (bool, error) {
	p.input.ObservedState.CordonedInstances = p.reconciler.cordonedInstances(ctx, p.log, p.ir, p.input)
	return true, nil
}

// resolveInstanceReadyTimeout settles the readiness window of this
// Component's operations: the Component's own lifecycle value, else the
// operator's. Every deadline the pass writes reads it, migration requests
// included, so they all expire by one rule.
func (p *replicaPass) resolveInstanceReadyTimeout(context.Context) (bool, error) {
	p.instanceReadyTimeout = workload.ResolveInstanceReadyTimeout(
		p.input.DesiredSpec.Lifecycle.InstanceReadyTimeout, p.settings.InstanceReadyTimeout)
	return true, nil
}

// syncMigrationRecords brings status.migrations up to date before any
// request is accepted: in-flight records of the migration table that status
// does not carry yet are imported, so they resume through the status path,
// and terminal records older than the audit window are removed. With no
// window configured nothing ages out.
func (p *replicaPass) syncMigrationRecords(ctx context.Context) (bool, error) {
	var recordWindow time.Duration
	if p.settings.MigrationAudit != nil {
		recordWindow = p.settings.MigrationAudit.Window
	}
	if err := p.reconciler.syncMigrationEntries(ctx, p.log, p.ir, p.parent, p.instanceReadyTimeout, recordWindow); err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: sync migration entries: %w", err)
	}
	return true, nil
}

// acceptMigrationRequests turns the parent's migration request annotations
// into status.migrations entries: a valid request becomes an entry with a
// Started row in the migration table, an invalid one a terminal Failed row,
// and the annotation is removed either way. The mode is the effective one
// the dispatcher reads (Never refuses at acceptance). An accepted request
// requeues; otherwise the engine input's copy of the records is refreshed,
// because the input was built before the import above.
func (p *replicaPass) acceptMigrationRequests(ctx context.Context) (bool, error) {
	mode := workload.MigrationModeOrDefault(p.input.DesiredSpec.Lifecycle.MigrationPolicy)
	requeue, err := p.reconciler.consumeMigrationRequests(ctx, p.log, p.ir, p.parent, mode, p.instanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: consume migration requests: %w", err)
	}
	if requeue {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	p.input.ObservedState.Migrations = migrationsFromIR(p.ir)
	return true, nil
}

// holdDeadlinesWhilePaused stops every in-flight operation deadline of a
// paused replica now, before revision creation, the rollback read or the
// plan can fail, so a failure cannot leave a clock running. The publication
// at the end of the pass does the usual deadline bookkeeping and restarts
// the clocks after an unpause.
func (p *replicaPass) holdDeadlinesWhilePaused(ctx context.Context) (bool, error) {
	if !p.input.DesiredSpec.Paused {
		return true, nil
	}
	if err := p.reconciler.reconcileHeldDeadlines(ctx, p.ir, nil, true, 0); err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: park paused operation deadlines: %w", err)
	}
	return true, nil
}

// ensureSpecRevision ensures the ControllerRevision of the rendered spec,
// the spec target. It is reported as updateRevision even during a rollback,
// when the Instances roll toward another revision, so a new spec stays
// visible to the service and its canary. A same-name revision with other
// data raises the collision count and retries. A spec with no pod template
// has no revision.
func (p *replicaPass) ensureSpecRevision(ctx context.Context) (bool, error) {
	if p.input.DesiredSpec.PodSpec == nil {
		return true, nil
	}
	target, err := p.reconciler.ensureRevisionWithCollisionRetry(ctx, p.ir, p.input)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: ensure revision (ir=%s/%s): %w",
			p.ir.Namespace, p.ir.Name, err)
	}
	p.specTarget = target
	return true, nil
}

// resolveRollbackTarget makes the revision spec.pacing.rollbackToRevision
// names the roll target when this replica controls it: the desired template
// is rendered from the revision's stored payload and the forward roll drains
// the Instances onto it, while updateRevision keeps naming the spec target.
// A revision that is gone, or carries no pod template, leaves the spec
// target as the roll target.
func (p *replicaPass) resolveRollbackTarget(ctx context.Context) (bool, error) {
	p.rollTarget = p.specTarget
	cr, err := p.reconciler.rollbackRevision(ctx, p.ir, workloadtypes.EventTarget(p.input))
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: %w", err)
	}
	if cr == nil {
		return true, nil
	}
	payload, err := revision.PayloadFromControllerRevision(cr)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: rollback revision payload (ir=%s/%s): %w", p.ir.Namespace, p.ir.Name, err)
	}
	if payload == nil || payload.PodSpec == nil {
		return true, nil
	}
	if err := p.reconciler.applyRollbackPayload(ctx, p.ir, &p.input.DesiredSpec, payload, cr); err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: apply rollback revision %s: %w", cr.Name, err)
	}
	p.rollTarget = cr
	p.rollbackRevision = cr
	return true, nil
}

// wireCoordinationGates installs the coordination gates on the input. The
// gates orient by the roll target, so they are wired only now that it is
// resolved.
func (p *replicaPass) wireCoordinationGates(ctx context.Context) (bool, error) {
	p.reconciler.wireCoordinationGates(ctx, &p.input, p.ir, p.parent, p.coordDefaults, p.rollTarget)
	return true, nil
}

// readRevisionWidths reads the worker width recorded by every revision the
// rows reference and by the roll target. Each Instance renders the width its
// own revision recorded, so the plan needs them all.
func (p *replicaPass) readRevisionWidths(ctx context.Context) (bool, error) {
	rollTargetName := ""
	if p.rollTarget != nil {
		rollTargetName = p.rollTarget.Name
	}
	widths, err := revision.WorkerSizesByRevision(ctx, p.reconciler.Client, p.ir.Namespace,
		revision.ReferencedRevisionNames(p.input.ObservedState.InstanceStatuses, rollTargetName))
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: read revision widths (ir=%s/%s): %w",
			p.ir.Namespace, p.ir.Name, err)
	}
	p.input.ObservedState.WorkerSizeByRevision = widths
	return true, nil
}

// buildPlan builds the plan of the pass from the desired spec and the
// observed rows, and overlays the two operator windows the plan does not
// carry: the gang admission bound (nil leaves the derived PodGroup timeout
// unclamped) and the readiness window (zero opens operations with no
// deadline).
func (p *replicaPass) buildPlan(context.Context) (bool, error) {
	plan, err := workload.BuildPlan(v1beta1convert.ComponentTypeToWorkload(p.ir.Spec.Component), p.input.DesiredSpec, p.input.ObservedState)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: build plan (ir=%s/%s): %w",
			p.ir.Namespace, p.ir.Name, err)
	}
	plan.GangScheduleTimeout = p.settings.GangScheduleTimeout
	plan.InstanceReadyTimeout = p.instanceReadyTimeout
	p.plan = plan
	return true, nil
}

// clearIdleScaleDownGauges sets the scale-down gauges of a Component with
// no delete in flight to zero, so a finished wave does not read as open.
func (p *replicaPass) clearIdleScaleDownGauges(context.Context) (bool, error) {
	if scaleDownPodObservationRequired(p.input, p.plan) {
		return true, nil
	}
	obsmetrics.SetScaleDownActivePods(p.ir.Namespace, p.ir.NamePrefix(), string(p.ir.Spec.Component), 0)
	obsmetrics.SetScaleDownDeferredInstances(p.ir.Namespace, p.ir.NamePrefix(), string(p.ir.Spec.Component), 0)
	return true, nil
}

// installStatusPublication turns the status publication on: from here
// every return of the pass, early or not, promotes or clears the current
// revision, writes the counters, the conditions and the hold, and sweeps
// old revisions. The publication also stops the readiness deadlines of a
// paused replica, so a failed live read or PodGroup step must not skip it.
// It notes the Instances that have a row now, so the publication can tell
// a row this pass removed from one that never was.
func (p *replicaPass) installStatusPublication(context.Context) (bool, error) {
	p.instancesAtStart = instanceIndices(p.ir.Status.InstanceStatuses)
	p.publishOnReturn = true
	return true, nil
}

// logRollbackTarget logs the revision the Instances roll back to.
func (p *replicaPass) logRollbackTarget(context.Context) (bool, error) {
	if p.rollbackRevision != nil {
		p.log.Info("Rollback: rolling Instances back to revision", "revision", p.rollbackRevision.Name)
	}
	return true, nil
}

// snapshotLivePodsForScaleDown lists the Component's pods live when a
// scale-down or a delete is in flight, so the delete decides on the
// apiserver's picture, not the cache's. A pod this replica owns without a
// valid instance label makes the picture unusable: the pass ends with an
// error and publishes nothing, so no effect of this pass reaches status.
func (p *replicaPass) snapshotLivePodsForScaleDown(ctx context.Context) (bool, error) {
	if !scaleDownPodObservationRequired(p.input, p.plan) {
		return true, nil
	}
	pods, err := query.LiveListPodsForComponent(ctx, p.reconciler.APIReader, p.ir.Namespace, p.ir.NamePrefix(),
		v1beta1convert.ComponentTypeToWorkload(p.ir.Spec.Component))
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: observe scale-down pods (component=%s): %w", p.ir.Spec.Component, err)
	}
	owned := podsControlledBy(pods, p.ir.UID)
	if invalid := invalidInstanceIndexPodCount(owned); invalid > 0 {
		p.publishOnReturn = false
		return false, fmt.Errorf(
			"InferenceReplica reconciler: authoritative scale-down snapshot contains %d UID-owned component pod(s) without a valid %s label; refusing scale-down effects",
			invalid, query.LabelInstanceIdx)
	}
	p.input.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
		OwnerUID:   p.ir.UID,
		Pods:       owned,
		ByInstance: query.BucketPodsByInstanceIdx(owned),
	}
	return true, nil
}

// reportReadyTimeout reports whether this Component has a readiness window,
// past every guard that refuses to act, so a pass that refuses writes
// nothing at all.
func (p *replicaPass) reportReadyTimeout(ctx context.Context) (bool, error) {
	p.reconciler.reportInstanceReadyTimeout(ctx, p.log, p.ir, p.input, p.plan.InstanceReadyTimeout)
	return true, nil
}

// recordUpdateRevision writes the spec target as updateRevision before any
// pod renders it. A pass that ends between creating a pod and the
// publication then leaves pods of the new revision beside a status that
// already names it, never beside one still reporting the previous revision
// as current and done. The record moves the pair as one pair, so a rollback
// onto the current revision reads open from this write on.
func (p *replicaPass) recordUpdateRevision(ctx context.Context) (bool, error) {
	if p.specTarget == nil || p.ir.Status.UpdateRevision == p.specTarget.Name {
		return true, nil
	}
	if err := buildRecordUpdateRevision(p.reconciler.statusWriter(), p.reconciler.liveReader(), p.reconciler.Client, p.ir)(ctx, p.specTarget.Name); err != nil {
		if errors.Is(err, workloadtypes.ErrStatusOwnerGone) {
			return p.end(ctrl.Result{}, nil)
		}
		if errors.Is(err, workloadtypes.ErrStatusMutationPrecondition) || apierrors.IsConflict(err) {
			return p.end(ctrl.Result{Requeue: true}, nil)
		}
		return false, fmt.Errorf("InferenceReplica reconciler: record update revision (ir=%s/%s): %w",
			p.ir.Namespace, p.ir.Name, err)
	}
	p.input.ObservedState.UpdateRevision = p.ir.Status.UpdateRevision
	return true, nil
}

// resolvePeerRevisions resolves, for every pod rendered this pass, the roll
// target revision of each serving peer and creates that revision's
// per-revision Services before any pod that names them exists. While a
// peer's projection or status lags this Component's parent generation the
// pairing is unknowable, so the pass renders nothing, publishes its status
// and retries.
func (p *replicaPass) resolvePeerRevisions(ctx context.Context) (bool, error) {
	if !coordination.PeerEnvDeclared(p.parent) {
		return true, nil
	}
	peers, hold, err := p.reconciler.resolvePeerRevisions(ctx, p.ir, p.parent, p.rollTarget)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: resolve peer revisions (component=%s): %w", p.ir.Spec.Component, err)
	}
	if hold != "" {
		p.log.V(1).Info("Holding dispatch until peer revisions are resolvable", "reason", hold)
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	if err := p.reconciler.ensurePeerRevisionServices(ctx, p.parent, peers); err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: ensure peer per-revision services (component=%s): %w", p.ir.Spec.Component, err)
	}
	p.peerRevisionFor = peers.hashFor
	return true, nil
}

// buildEngineDeps gathers what the engine calls out to: the clients, the
// recorder, the expectations cache, the clock and the render hook. The hook
// overlays each peer's endpoint and per-revision endpoint onto every
// rendered pod, so the prefill and decode sides address each other by
// stable DNS and by the revision they pair with; it renders nothing when the
// parent has no rollout groups or is nil.
func (p *replicaPass) buildEngineDeps(context.Context) (bool, error) {
	p.deps = workloadtypes.Deps{
		Client:       p.reconciler.Client,
		APIReader:    p.reconciler.APIReader,
		Recorder:     p.reconciler.Recorder,
		Expectations: p.reconciler.Expectations,
		Clock:        p.reconciler.Clock,
		RenderHook:   ISVCRenderHook(p.parent, p.peerRevisionFor),
	}
	return true, nil
}

// observePodGroups reads the PodGroups this owner controls when a gang
// decision needs the apiserver's picture, removes the PodGroups left by
// Instances that became single-pod, and installs the per-Instance resource
// finalization that removes an Instance's PodGroup with it. A removal still
// in flight requeues on the scale-down cadence.
func (p *replicaPass) observePodGroups(ctx context.Context) (bool, error) {
	if !p.input.DesiredSpec.GangSchedulingAvailable {
		return true, nil
	}
	observe, err := p.reconciler.requiresAuthoritativePodGroupInventory(ctx, p.input, p.plan)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: check pod group inventory requirement (component=%s): %w", p.ir.Spec.Component, err)
	}
	if observe {
		p.podGroupInventory, err = workloadgang.ObservePodGroups(ctx, p.deps.Reader(), p.input.OwnerObject)
		if err != nil {
			return false, fmt.Errorf("InferenceReplica reconciler: observe pod groups (component=%s): %w", p.ir.Spec.Component, err)
		}
		blocked, cleanupErr := p.reconciler.reconcileStaleSinglePodGroups(ctx, p.ir, &p.input, p.plan, p.podGroupInventory)
		if cleanupErr != nil {
			return false, cleanupErr
		}
		if blocked {
			return p.end(scaleDownRequeueResult(p.reconciler.ScaleDownRequeueInterval), nil)
		}
	}
	p.input.FinalizeInstanceResources = workloadgang.BuildFinalizeInstanceResources(
		p.deps.Client, p.deps.Reader(), p.podGroupInventory, p.input.OwnerObject, p.input.Key.OwnerName, p.plan.Component)
	return true, nil
}

// ensurePodGroups creates the PodGroups before the first pod of any
// multi-pod Instance, or the gang scheduler rejects the pods and they stay
// Pending. It is a no-op for single-pod Components and when the PodGroup CRD
// is absent, when the engine writes GangSchedulingUnavailable instead. A
// gang surge's PodGroup is created inline, just before its pods, through the
// dependency installed here. A PodGroup name this owner cannot write is
// charged to the Instance that wanted it, so one unusable PodGroup never
// stops the whole Component.
func (p *replicaPass) ensurePodGroups(ctx context.Context) (bool, error) {
	state := workloadgang.PodGroupReconcileState{
		Inventory:     p.podGroupInventory,
		TerminalOwned: TerminalFinalizationOwned(p.input.ObservedState),
	}
	p.deps.EnsureGangPodGroup = workloadgang.EnsureSurgePodGroupWithState(p.deps, state)
	effectiveTopology, err := workloadgang.EnsurePodGroupsWithState(ctx, p.deps, p.input, p.plan, state)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: ensure pod groups (component=%s): %w", p.ir.Spec.Component, err)
	}
	p.plan.InstanceTopologyKeys = effectiveTopology
	return true, nil
}

// provisionSlices provisions the TPU slices the Instances of this pass
// need, when the manager was configured to provision them.
func (p *replicaPass) provisionSlices(ctx context.Context) (bool, error) {
	slices, err := p.reconciler.provisionSlices(ctx, p.ir, &p.deps, &p.input)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: provision TPU slices (component=%s): %w", p.ir.Spec.Component, err)
	}
	p.slices = slices
	return true, nil
}

// runEngine runs the engine once over the input and the plan toward the
// roll target, and records what it returned.
func (p *replicaPass) runEngine(ctx context.Context) (bool, error) {
	p.result, p.err = workload.Reconcile(ctx, p.deps, p.input, p.plan, p.rollTarget)
	return true, nil
}

// tendSlices recovers the TPU slices of Instances whose slice was lost and
// sweeps the slices no Instance needs, after an engine pass that returned no
// error. A slice failure replaces the engine's result with its own error.
func (p *replicaPass) tendSlices(ctx context.Context) (bool, error) {
	if p.err != nil {
		return true, nil
	}
	if err := p.reconciler.recoverLostSlices(ctx, p.ir, p.input, p.slices); err != nil {
		p.result = ctrl.Result{}
		p.err = fmt.Errorf("InferenceReplica reconciler: recover lost TPU slices (component=%s): %w", p.ir.Spec.Component, err)
		return true, nil
	}
	if err := p.slices.sweep(ctx, p.input, p.plan); err != nil {
		p.result = ctrl.Result{}
		p.err = fmt.Errorf("InferenceReplica reconciler: sweep TPU slices (component=%s): %w", p.ir.Spec.Component, err)
	}
	return true, nil
}

// reconcileHeadlessService keeps the per-Component headless Service, which
// gives every pod a stable name for peer discovery during gang start and for
// any proxy that wants per-pod addresses, and ends the pass with the
// engine's result. It runs after the engine so the Service exists by the
// time pods become Ready. A Service failure is reported only when the engine
// succeeded; otherwise it is logged and the engine's error stands.
func (p *replicaPass) reconcileHeadlessService(ctx context.Context) (bool, error) {
	if serr := workloadservice.ReconcileHeadlessService(ctx, p.reconciler.Client, buildHeadlessServiceSpec(p.ir)); serr != nil {
		if p.err == nil {
			p.result = ctrl.Result{}
			p.err = fmt.Errorf("InferenceReplica reconciler: reconcile headless service: %w", serr)
		} else {
			p.log.V(1).Error(serr, "Headless Service reconcile also failed; primary error preserved")
		}
	}
	return p.end(p.result, p.err)
}

// publishStatus is the end of every pass past installStatusPublication, run
// on every return with the result the pass holds. It promotes the current
// revision when every Instance is on the spec target, writes the counters,
// the conditions and the hold, sweeps old revisions and logs the revision
// transitions. A stale read behind a write requeues in place of the result;
// a write failure is reported only when the pass had no error of its own,
// so a status failure never hides an engine failure. A pass with no error
// comes back for the earliest pod that crosses its minReadySeconds window,
// which raises the Available counters with no watch event.
func (p *replicaPass) publishStatus(ctx context.Context, result ctrl.Result, err error) (ctrl.Result, error) {
	if ended, promoteResult, promoteErr := p.promoteCurrentRevision(ctx, err); ended {
		return promoteResult, promoteErr
	}
	nextAvailableIn, serr := p.reconciler.aggregateAndWriteStatus(ctx, p.ir, p.plan, p.specTarget, p.holdObserved, p.hold, p.instancesAtStart)
	if errors.Is(serr, workloadtypes.ErrStatusMutationPrecondition) {
		return ctrl.Result{Requeue: true}, nil
	}
	if serr != nil && err != nil {
		p.log.V(1).Error(serr, "Status write also failed; primary error preserved")
	}
	// The retention sweep runs on a failed write too: it works from the
	// last committed status and runs again next pass.
	p.reconciler.sweepRevisions(ctx, p.ir)
	p.logRevisionTransitions()
	p.published = true
	if serr != nil && err == nil {
		if apierrors.IsConflict(serr) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: write status: %w", serr)
	}
	if err == nil {
		result = foldRequeueAfter(result, nextAvailableIn)
	}
	return result, err
}

// promoteCurrentRevision writes currentRevision against the spec target
// before the status write, so the Ready condition reads the promoted value.
// It ends the publication, in place of the pass's result, on a stale read
// (requeue) and, when the pass had no error of its own, on a conflict
// (requeue) or another failure (the error); behind a pass error the failure
// is logged and the pass error stands.
func (p *replicaPass) promoteCurrentRevision(ctx context.Context, passErr error) (bool, ctrl.Result, error) {
	if p.specTarget == nil {
		return false, ctrl.Result{}, nil
	}
	perr := buildPromoteCurrentRevision(p.reconciler.statusWriter(), p.reconciler.liveReader(), p.reconciler.Client, p.ir)(ctx, p.specTarget.Name)
	if perr == nil {
		return false, ctrl.Result{}, nil
	}
	if errors.Is(perr, workloadtypes.ErrStatusMutationPrecondition) {
		return true, ctrl.Result{Requeue: true}, nil
	}
	if passErr == nil {
		if apierrors.IsConflict(perr) {
			return true, ctrl.Result{Requeue: true}, nil
		}
		return true, ctrl.Result{}, fmt.Errorf("InferenceReplica reconciler: promote current revision: %w", perr)
	}
	p.log.V(1).Error(perr, "CurrentRevision promotion also failed; primary error preserved")
	return false, ctrl.Result{}, nil
}

// logRevisionTransitions logs a current revision restored, promoted or
// cleared, by comparing the revision pair the publication left with the pair
// the pass read.
func (p *replicaPass) logRevisionTransitions() {
	switch {
	case p.priorClearedRevision != "" && p.ir.Status.CurrentRevision == p.priorClearedRevision:
		p.log.Info("Rollout closed; CurrentRevision restored once every Instance was back on the revision it was withdrawn from",
			"currentRevision", p.ir.Status.CurrentRevision)
	case p.priorCurrentRevision != p.ir.Status.CurrentRevision && p.ir.Status.CurrentRevision != "":
		p.log.Info("Rollout complete; CurrentRevision promoted",
			"previousCurrentRevision", p.priorCurrentRevision,
			"currentRevision", p.ir.Status.CurrentRevision)
	case p.priorCurrentRevision != "" && p.ir.Status.CurrentRevision == "":
		p.log.Info("Rollout reopened; CurrentRevision withdrawn while an Instance still runs, or is still pinned to, another revision",
			"previousCurrentRevision", p.priorCurrentRevision,
			"updateRevision", p.ir.Status.UpdateRevision)
	}
}

// resolveUpdateRetryPolicy loads lifecycle.updateRetry from the operator
// ConfigMap (through the shared short-TTL ConfigCache). Absent, invalid,
// or unloadable config resolves to nil — the workload layer fails safe
// (the first same-target failure Holds) — with one V(1) log naming the
// cause; there is never a silent in-code fallback policy.
func (r *Reconciler) resolveUpdateRetryPolicy(log logr.Logger) *workloadtypes.RetryPolicy {
	if r.Clientset == nil {
		log.V(1).Info("update retry policy unresolved: no clientset wired; failing safe (Held on first same-target failure)")
		return nil
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("update retry policy unresolved: lifecycle config load failed; failing safe (Held on first same-target failure)", "error", err.Error())
		return nil
	}
	if cfg == nil || cfg.UpdateRetry == nil {
		log.V(1).Info("update retry policy unconfigured (no lifecycle.updateRetry in inferenceservice-config); failing safe (Held on first same-target failure)")
		return nil
	}
	policy, err := cfg.UpdateRetry.ToPolicy()
	if err != nil {
		log.V(1).Info("update retry policy invalid; failing safe (Held on first same-target failure)", "error", err.Error())
		return nil
	}
	return policy
}

func scaleDownPodObservationRequired(input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan) bool {
	if len(workload.ScaleDownExtras(input.ObservedState.InstanceStatuses, plan)) > 0 {
		return true
	}
	for _, status := range input.ObservedState.InstanceStatuses {
		if status.Phase == workloadtypes.InstancePhaseDeleting && status.Operation != nil && status.Operation.Type == workloadtypes.InstanceOperationDelete {
			return true
		}
	}
	return false
}

func invalidInstanceIndexPodCount(pods []*corev1.Pod) int {
	invalid := 0
	for _, pod := range pods {
		if pod == nil {
			invalid++
			continue
		}
		raw, found := pod.Labels[query.LabelInstanceIdx]
		idx, err := strconv.ParseInt(raw, 10, 32)
		if !found || err != nil || idx < 0 {
			invalid++
		}
	}
	return invalid
}

func scaleDownRequeueResult(interval time.Duration) ctrl.Result {
	if interval <= 0 {
		return ctrl.Result{}
	}
	return ctrl.Result{RequeueAfter: interval}
}

// foldRequeueAfter schedules a wake no later than requeueAfter: an immediate
// Requeue is left alone, otherwise the earlier RequeueAfter wins. A
// non-positive requeueAfter asks for nothing.
func foldRequeueAfter(result ctrl.Result, requeueAfter time.Duration) ctrl.Result {
	if requeueAfter <= 0 || (result.Requeue && result.RequeueAfter == 0) {
		return result
	}
	if result.RequeueAfter == 0 || requeueAfter < result.RequeueAfter {
		result.RequeueAfter = requeueAfter
	}
	return result
}

// requiresAuthoritativePodGroupInventory keeps steady single-pod workloads on
// the owner-UID cache index while preserving live proofs for lifecycle work.
func (r *Reconciler) requiresAuthoritativePodGroupInventory(
	ctx context.Context,
	input workloadtypes.ReconcileInput,
	plan workloadtypes.ComponentPlan,
) (bool, error) {
	for _, instance := range plan.Instances {
		if instance.TotalPods() > 1 {
			return true, nil
		}
	}
	if scaleDownPodObservationRequired(input, plan) {
		return true, nil
	}
	if len(TerminalFinalizationOwned(input.ObservedState)) > 0 {
		return true, nil
	}
	return workloadgang.CachedOwnerHasPodGroups(ctx, r.Client, input.OwnerObject)
}

func podsControlledBy(pods []*corev1.Pod, ownerUID types.UID) []*corev1.Pod {
	owned := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		controller := metav1.GetControllerOf(pod)
		if controller != nil && controller.UID == ownerUID {
			owned = append(owned, pod)
		}
	}
	return owned
}

func (r *Reconciler) reconcileStaleSinglePodGroups(
	ctx context.Context,
	ir *v1beta1.InferenceReplica,
	input *workloadtypes.ReconcileInput,
	plan workloadtypes.ComponentPlan,
	inventory *workloadgang.PodGroupInventory,
) (bool, error) {
	if inventory == nil || !inventory.Available() {
		return false, nil
	}
	terminalOwned := TerminalFinalizationOwned(input.ObservedState)
	type stalePodGroup struct {
		name  string
		index int32
	}
	var groups []stalePodGroup
	for _, instance := range plan.Instances {
		if instance.TotalPods() != 1 {
			continue
		}
		if _, terminal := terminalOwned[instance.Index]; terminal {
			continue
		}
		name := query.PodGroupName(input.Key.OwnerName, plan.Component, instance.Index)
		if _, found := inventory.OwnedByName(name); found {
			groups = append(groups, stalePodGroup{name: name, index: instance.Index})
		}
	}
	if len(groups) == 0 {
		return false, nil
	}
	if input.AuthoritativePods == nil {
		pods, err := query.LiveListPodsForComponent(ctx, r.APIReader, ir.Namespace, ir.NamePrefix(), plan.Component)
		if err != nil {
			return false, fmt.Errorf("InferenceReplica reconciler: observe pods for stale PodGroup cleanup: %w", err)
		}
		owned := podsControlledBy(pods, ir.UID)
		input.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
			OwnerUID:   ir.UID,
			Pods:       owned,
			ByInstance: query.BucketPodsByInstanceIdx(owned),
		}
	}
	if invalid := invalidInstanceIndexPodCount(input.AuthoritativePods.Pods); invalid > 0 {
		return false, fmt.Errorf(
			"InferenceReplica reconciler: authoritative stale-PodGroup snapshot contains %d UID-owned component pod(s) without a valid %s label; refusing PodGroup effects",
			invalid, query.LabelInstanceIdx)
	}
	referenced := make(map[string]struct{})
	for _, pod := range input.AuthoritativePods.Pods {
		if pod != nil && pod.Labels[query.LabelPodGroup] != "" {
			referenced[pod.Labels[query.LabelPodGroup]] = struct{}{}
		}
	}
	podsByInstance := input.AuthoritativePods.ByInstance
	if podsByInstance == nil {
		podsByInstance = query.BucketPodsByInstanceIdx(input.AuthoritativePods.Pods)
	}
	footprint := workloadtypes.ScaleDownPodFootprint(input.ObservedState.InstanceStatuses, podsByInstance)
	statusIndices := make(map[int32]struct{}, len(input.ObservedState.InstanceStatuses))
	for _, row := range input.ObservedState.InstanceStatuses {
		statusIndices[row.Index] = struct{}{}
	}
	for _, group := range groups {
		_, hasStatus := statusIndices[group.index]
		if !hasStatus && len(podsByInstance[group.index]) == 0 && footprint < math.MaxInt32 {
			footprint++
		}
	}
	budget, err := workloadtypes.ResolveScaleDownPodBatchSize(input.ScaleDownPodBatchSize, footprint)
	if err != nil {
		return false, fmt.Errorf("InferenceReplica reconciler: resolve stale PodGroup cleanup budget: %w", err)
	}
	pending := int32(0)
	for _, entry := range inventory.OwnedEntries() {
		if entry.PodGroup.DeletionTimestamp != nil || inventory.DeleteAccepted(entry.Name) {
			pending++
		}
	}
	deleted := int32(0)
	blocked := false
	for _, group := range groups {
		pg, _ := inventory.OwnedByName(group.name)
		if pg.DeletionTimestamp != nil || inventory.DeleteAccepted(group.name) {
			blocked = true
			continue
		}
		if _, inUse := referenced[group.name]; inUse {
			continue
		}
		// The PodGroup name is the direct reference. Its planned index is
		// conservative recovery evidence when a Pod lost that reference.
		if len(podsByInstance[group.index]) > 0 {
			continue
		}
		if budget != nil && pending+deleted >= *budget {
			blocked = true
			continue
		}
		if err := inventory.DeleteOwnedName(ctx, r.Client, group.name); err != nil {
			return false, fmt.Errorf("InferenceReplica reconciler: delete stale PodGroup %s: %w", group.name, err)
		}
		deleted++
		blocked = true
	}
	return blocked, nil
}

// TerminalFinalizationOwned is the set of Instance indices whose PodGroup a
// terminal lifecycle owner finalizes itself: a draining migration source, a
// Deleting row, a gang-surge cleanup marker, and a drained or failed surge pair.
func TerminalFinalizationOwned(observed workloadtypes.WorkloadObservedState) map[int32]struct{} {
	owned := make(map[int32]struct{})
	for _, record := range observed.Migrations {
		if record.Phase == workloadtypes.MigrationPhaseDraining {
			owned[record.SourceInstance] = struct{}{}
		}
	}
	for _, status := range observed.InstanceStatuses {
		if status.Operation == nil {
			continue
		}
		if status.Phase == workloadtypes.InstancePhaseDeleting && status.Operation.Type == workloadtypes.InstanceOperationDelete {
			owned[status.Index] = struct{}{}
		}
		if status.Operation.Type == workloadtypes.InstanceOperationUpdate && status.Operation.Step == workloadtypes.UpdateStepGangSurgeTargetCleanup {
			owned[status.Index] = struct{}{}
		}
		if status.Operation.Type == workloadtypes.InstanceOperationUpdate && status.Operation.SurgeIndex != nil {
			if status.Operation.Step == workloadtypes.UpdateStepSurgeDrain || status.Operation.Step == workloadtypes.UpdateStepSurgeDrainSettle {
				owned[status.Index] = struct{}{}
			}
			if status.Phase == workloadtypes.InstancePhaseFailed {
				owned[*status.Operation.SurgeIndex] = struct{}{}
			}
		}
	}
	return owned
}

// lifecycleSettings are the tunables one reconcile resolves from the
// operator ConfigMap. Every field is zero when unconfigured, which each
// consumer reads as "this window does not exist"; there are no in-code
// defaults behind them.
type lifecycleSettings struct {
	// InstanceReadyTimeout is the operator-wide readiness backstop a
	// Component that sets no lifecycle.instanceReadyTimeout of its own
	// falls back to. Zero leaves the Component opening operations with no
	// deadline.
	InstanceReadyTimeout time.Duration
	// StuckPod bounds a pod parked in a terminal kubelet waiting state.
	// Zero skips fast escalation; the InstanceReadyTimeout backstop still
	// fires.
	StuckPod time.Duration
	// Unschedulable bounds a pod the scheduler cannot place. Zero leaves
	// the hold parking the clock until an operator resolves it.
	Unschedulable time.Duration
	// GangDomainFull bounds a gang member the scheduler cannot place because
	// the domain its gang is held to cannot take it. Zero leaves such a
	// hold on Unschedulable.
	GangDomainFull time.Duration
	// AbandonedReplacement bounds the termination grace of a replacement
	// pod a rollout abandons before it ever served. Zero leaves such a pod
	// its own grace.
	AbandonedReplacement time.Duration
	// GangScheduleTimeout bounds the PodGroup schedule timeout derived
	// from InstanceReadyTimeout. Nil passes the derived value through to
	// the scheduler.
	GangScheduleTimeout *workloadtypes.GangScheduleTimeoutClamp
	// MigrationAudit bounds migration admission and the status-record
	// horizon. Nil holds every migration request and ages out no record.
	MigrationAudit *workloadtypes.MigrationAuditPolicy
	// Requeue is the dispatcher's wake-up cadence. Zero fields leave a
	// pass with work left to the controller's rate-limited backoff.
	Requeue workloadtypes.RequeueIntervals
	// RetryBlockHistoryLimit caps the RetryBlocks kept for superseded
	// revisions. Nil keeps every historical block.
	RetryBlockHistoryLimit *int32
}

// resolveLifecycleSettings loads the tunables from the operator ConfigMap
// in one read. Absent, invalid, or unloadable config resolves the
// affected field to 0 — that window is then disabled — with one V(1) log
// naming the cause; unconfigured is never an error, and there is never a
// silent in-code fallback value.
func (r *Reconciler) resolveLifecycleSettings(log logr.Logger) lifecycleSettings {
	cfg, err := r.loadLifecycleConfig(log, "lifecycle settings")
	if cfg == nil || err != nil {
		return lifecycleSettings{}
	}
	var settings lifecycleSettings
	settings.InstanceReadyTimeout = r.resolveConfiguredInstanceReadyTimeout(log)
	if settings.StuckPod, err = cfg.ToGracePeriod(); err != nil {
		log.V(1).Info("stuck-pod grace invalid; skipping fast escalation this pass", "error", err.Error())
	}
	if settings.Unschedulable, err = cfg.ToUnschedulableGracePeriod(); err != nil {
		log.V(1).Info("unschedulable grace invalid; scheduler-hold escalation disabled this pass", "error", err.Error())
	}
	if settings.GangDomainFull, err = cfg.ToGangDomainFullGracePeriod(); err != nil {
		log.V(1).Info("full-domain grace invalid; a gang held to a full domain waits on the unschedulable grace this pass", "error", err.Error())
	}
	if settings.AbandonedReplacement, err = cfg.ToAbandonedReplacementGracePeriod(); err != nil {
		log.V(1).Info("abandoned-replacement grace invalid; an abandoned replacement keeps its own grace this pass", "error", err.Error())
	}
	if settings.StuckPod == 0 {
		log.V(1).Info("stuck-pod grace unconfigured (no lifecycle.stuckPodGracePeriod); skipping fast escalation this pass")
	}
	if settings.Unschedulable == 0 {
		log.V(1).Info("unschedulable grace unconfigured (no lifecycle.unschedulableGracePeriod); scheduler-hold escalation disabled this pass")
	}
	if settings.AbandonedReplacement == 0 {
		log.V(1).Info("abandoned-replacement grace unconfigured (no lifecycle.abandonedReplacementGracePeriod); an abandoned replacement keeps its own termination grace")
	}
	if settings.GangScheduleTimeout, err = cfg.GangScheduleTimeout.ToClamp(); err != nil {
		log.V(1).Info("gang schedule timeout invalid; the derived timeout is unclamped this pass", "error", err.Error())
	}
	if settings.MigrationAudit, err = cfg.Audit.ToPolicy(); err != nil {
		log.V(1).Info("migration audit policy invalid; no migration is admitted this pass", "error", err.Error())
	}
	if settings.Requeue, err = cfg.Requeue.ToIntervals(); err != nil {
		log.V(1).Info("requeue cadence invalid; passes fall back to rate-limited backoff", "error", err.Error())
	}
	if settings.RetryBlockHistoryLimit, err = cfg.ToRetryBlockHistoryLimit(); err != nil {
		log.V(1).Info("retry-block history limit invalid; historical blocks are kept this pass", "error", err.Error())
	}
	if settings.GangScheduleTimeout == nil {
		log.V(1).Info("gang schedule timeout unconfigured (no lifecycle.gangScheduleTimeout); the timeout derived from InstanceReadyTimeout reaches the scheduler unclamped")
	}
	if settings.MigrationAudit == nil {
		log.V(1).Info("migration capacity unconfigured (no lifecycle.audit); migration requests are held until it is set and terminal records are not aged out")
	}
	if settings.Requeue.Operation == 0 || settings.Requeue.Gate == 0 {
		log.V(1).Info("requeue cadence unconfigured (no lifecycle.requeue); a pass with work left requeues on rate-limited backoff")
	}
	if settings.RetryBlockHistoryLimit == nil {
		log.V(1).Info("retry-block history unconfigured (no lifecycle.retryBlockHistoryLimit); blocks for superseded revisions are kept")
	}
	return settings
}

// instanceReadyTimeoutUnsetMessage explains the Component-scoped
// InstanceReadyTimeoutUnconfigured condition and the Warning event that
// raises it.
const instanceReadyTimeoutUnsetMessage = "no instanceReadyTimeout is set on the Component or in lifecycle.instanceReadyTimeout; operations open with no readiness deadline"

// reportInstanceReadyTimeout surfaces the Component's effective readiness
// backstop as a condition: True (degraded) when neither the per-resource
// lifecycle value nor the operator's supplies one, False once either does.
// An unset window is legal — an Instance that never becomes Ready then waits
// for an operator instead of being failed — so it is reported, never an
// error. The Warning is edge-triggered off the standing condition, so it
// fires once per episode rather than every pass.
func (r *Reconciler) reportInstanceReadyTimeout(ctx context.Context, log logr.Logger,
	ir *v1beta1.InferenceReplica, input workloadtypes.ReconcileInput, timeout time.Duration) {
	cond := metav1.Condition{
		Type:               string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured),
		Status:             metav1.ConditionFalse,
		Reason:             string(workloadtypes.ReasonInstanceReadyTimeoutConfigured),
		Message:            fmt.Sprintf("Instance readiness deadline is %s", timeout),
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: ir.Generation,
	}
	if timeout <= 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = string(workloadtypes.ReasonInstanceReadyTimeoutUnconfigured)
		cond.Message = instanceReadyTimeoutUnsetMessage
		if !apimeta.IsStatusConditionTrue(ir.Status.Conditions, cond.Type) {
			workloadtypes.RecordWarning(r.Recorder, workloadtypes.EventTarget(input),
				workloadtypes.EventReasonInstanceReadyTimeoutUnconfigured,
				"OMENative component=%s: %s", ir.Spec.Component, instanceReadyTimeoutUnsetMessage)
		}
	}
	if input.WriteAggregateCondition == nil {
		return
	}
	if err := input.WriteAggregateCondition(ctx, cond); err != nil {
		log.V(1).Info("could not record the instance-ready timeout condition", "error", err.Error())
	}
}

// resolveConfiguredInstanceReadyTimeout loads lifecycle.instanceReadyTimeout
// from the operator ConfigMap. Absent, invalid, or unloadable config
// resolves to 0 — a Component that sets no readiness window of its own then
// opens operations with no deadline — with one V(1) log naming the cause;
// there is never an in-code fallback window.
func (r *Reconciler) resolveConfiguredInstanceReadyTimeout(log logr.Logger) time.Duration {
	cfg, err := r.loadLifecycleConfig(log, "instance-ready timeout")
	if cfg == nil || err != nil {
		return 0
	}
	timeout, terr := cfg.ToInstanceReadyTimeout()
	if terr != nil {
		log.V(1).Info("instance-ready timeout invalid; no operator-wide readiness backstop this pass", "error", terr.Error())
		return 0
	}
	if timeout == 0 {
		log.V(1).Info("instance-ready timeout unconfigured (no lifecycle.instanceReadyTimeout); a Component that sets none of its own opens operations with no deadline")
	}
	return timeout
}

// loadLifecycleConfig fetches the lifecycle block through the shared
// short-TTL cache, logging once at V(1) and returning nil when it cannot
// be read. purpose names the caller in that log so an operator can tell
// which resolver went unconfigured.
func (r *Reconciler) loadLifecycleConfig(log logr.Logger, purpose string) (*controllerconfig.LifecycleConfig, error) {
	if r.Clientset == nil {
		log.V(1).Info("lifecycle config unresolved: no clientset wired", "purpose", purpose)
		return nil, nil
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("lifecycle config load failed", "purpose", purpose, "error", err.Error())
		return nil, err
	}
	if cfg == nil {
		log.V(1).Info("no lifecycle block in inferenceservice-config", "purpose", purpose)
	}
	return cfg, nil
}

// resolveAutoMigrateBudget loads lifecycle.autoMigrate.maxAttempts from
// the operator ConfigMap. Absent, invalid, or unloadable config resolves
// to 0 — the deadline disposition's relocation branch is disabled (no
// migration without an operator budget) — with one V(1) log naming the
// cause; there is never a silent in-code fallback budget.
func (r *Reconciler) resolveAutoMigrateBudget(log logr.Logger) int32 {
	if r.Clientset == nil {
		log.V(1).Info("auto-migrate budget unresolved: no clientset wired; relocation branch disabled this pass")
		return 0
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("auto-migrate budget unresolved: lifecycle config load failed; relocation branch disabled this pass", "error", err.Error())
		return 0
	}
	if cfg == nil || cfg.AutoMigrate == nil {
		log.V(1).Info("auto-migrate budget unconfigured (no lifecycle.autoMigrate in inferenceservice-config); relocation branch disabled this pass")
		return 0
	}
	if err := cfg.AutoMigrate.Validate(); err != nil {
		log.V(1).Info("auto-migrate budget invalid; relocation branch disabled this pass", "error", err.Error())
		return 0
	}
	return cfg.AutoMigrate.MaxAttempts
}

// resolveForceDeletePolicy loads lifecycle.forceDelete from the operator
// ConfigMap (through the shared short-TTL ConfigCache). Absent, invalid,
// or unloadable config resolves to nil — the stuck-Terminating
// force-delete escalation is disabled this pass — with one V(1) log
// naming the cause; there is never a silent in-code fallback policy.
func (r *Reconciler) resolveForceDeletePolicy(log logr.Logger) *workloadtypes.ForceDeletePolicy {
	if r.Clientset == nil {
		log.V(1).Info("force-delete policy unresolved: no clientset wired; escalation disabled this pass")
		return nil
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("force-delete policy unresolved: lifecycle config load failed; escalation disabled this pass", "error", err.Error())
		return nil
	}
	if cfg == nil || cfg.ForceDelete == nil {
		log.V(1).Info("force-delete policy unconfigured (no lifecycle.forceDelete in inferenceservice-config); escalation disabled")
		return nil
	}
	policy, err := cfg.ForceDelete.ToPolicy()
	if err != nil {
		log.V(1).Info("force-delete policy invalid; escalation disabled this pass", "error", err.Error())
		return nil
	}
	return policy
}

// resolveCoordinationGroupDefaults loads the operator-configured group
// resolution fill-ins from the operator ConfigMap: the ratio tolerance
// default (coordination.defaultRatioTolerancePercent) and the pacing rules
// of a prefill/decode group (lifecycle.prefillDecodeRoll). Absent or
// unloadable config resolves to the zero value — a group that omits
// maintainRatio.tolerance then rolls with no drift bound, and the two
// sides of a prefill/decode group roll on their own budgets — with one
// V(1) log; there is never a silent in-code fallback.
func (r *Reconciler) resolveCoordinationGroupDefaults(log logr.Logger) coordination.GroupDefaults {
	if r.Clientset == nil {
		log.V(1).Info("coordination group defaults unresolved: no clientset wired; unfilled knobs use their unconfigured behavior")
		return coordination.GroupDefaults{}
	}
	defaults := coordination.GroupDefaults{}
	cfg, err := controllerconfig.NewCoordinationConfigCached(r.ConfigCache, r.Clientset)
	switch {
	case err != nil:
		log.V(1).Info("coordination group defaults unresolved: coordination config load failed; unfilled knobs use their unconfigured behavior", "error", err.Error())
	case cfg != nil:
		defaults.RatioTolerancePercent = cfg.DefaultRatioTolerancePercent
	}
	if lifecycle, err := r.loadLifecycleConfig(log, "prefill/decode roll rules"); err == nil && lifecycle != nil {
		keepRatio, firstSide, rerr := lifecycle.ToPrefillDecodeRoll()
		if rerr != nil {
			log.V(1).Info("prefill/decode roll firstSide invalid; both sides start their batch together this pass", "error", rerr.Error())
		}
		defaults.PrefillDecode = coordination.PrefillDecodeRules{KeepRatio: keepRatio, FirstSide: v1beta1.ComponentType(firstSide)}
	}
	return defaults
}

// resolveTeardownDeadline loads lifecycle.teardown.deadline from the
// operator ConfigMap. Returns (deadline, invalidReason): absent or
// unloadable config yields (nil, "") — strict hold, genuinely
// unconfigured — while config that is PRESENT but invalid yields
// (nil, <parse error>) so the strict-hold diagnostics can say why the
// deadline didn't apply instead of claiming none was configured. The
// invalid case logs at Info (not V(1)), once per resolve: an operator
// who wrote a bad duration must see it without raising verbosity.
func (r *Reconciler) resolveTeardownDeadline(log logr.Logger) (*time.Duration, string) {
	if r.Clientset == nil {
		log.V(1).Info("teardown deadline unresolved: no clientset wired; no deadline this pass")
		return nil, ""
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		log.V(1).Info("teardown deadline unresolved: lifecycle config load failed; no deadline this pass", "error", err.Error())
		return nil, ""
	}
	if cfg == nil || cfg.Teardown == nil {
		log.V(1).Info("teardown deadline unconfigured (no lifecycle.teardown in inferenceservice-config); no deadline")
		return nil, ""
	}
	deadline, err := cfg.Teardown.ToDeadline()
	if err != nil {
		log.Info("teardown deadline configured but invalid; strict hold applies", "error", err.Error())
		return nil, err.Error()
	}
	return deadline, ""
}

// reconcileRelocationDirectives owns the per-reconcile relocation-
// directive bookkeeping against the audit ledger (parent-ISVC-owned
// when resolvable, else IR-owned — same rule as buildReconcileInput's
// LedgerOwner):
//
//  1. Success-prune: AutoRecover records for any instance observed
//     Phase=Ready with no in-flight Operation are removed — the
//     instance proved its placement, so its exclusion memory and
//     relocation budget reset (mirror of the RetryBlock promote-prune;
//     done here, adapter-side, because the workload promote stampers
//     have no client handle, and gated on the ledger actually holding
//     entries so the steady-state hot path pays one cached GET at most —
//     the live reader is touched only when a prune must persist).
//  2. Projection: build the Instance-index → excluded-nodes map from
//     the remaining AutoRecover entries, each list bounded to the most
//     recent autoMigrateBudget entries and each node tagged with the
//     revision its directive was recorded for.
//
// All errors fail open with a V(1) log: the overlay is a placement
// steer, not a correctness gate, and the next pass re-derives it.
func (r *Reconciler) reconcileRelocationDirectives(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, autoMigrateBudget int32) map[int32][]workloadtypes.NodeExclusion {
	if r.Client == nil || ir == nil {
		return nil
	}
	// Success touch for the Auto visibility mirror: a Ready-no-op
	// instance confirms its newest un-Succeeded Auto record. The ledger
	// rows below are PRUNED on Ready while the status record persists
	// until the trim window — deliberate asymmetry: the ledger is
	// working memory for exclusions, the record is visible history. Runs
	// before (and independent of) the ledger so a crash between the
	// prune persist and this stamp still heals next pass.
	if err := r.stampAutoRelocationSuccess(ctx, ir); err != nil {
		log.V(1).Info("auto-relocation success stamp failed; will retry next pass", "error", err.Error())
	}
	owner := client.Object(ir)
	gvk := irGVK
	if parent != nil {
		owner = parent
		gvk = isvcGVK
	}
	// Cached read for the projection path: the overlay is a placement
	// steer re-derived every pass, so cache lag at worst delays an
	// exclusion by one sync. r.APIReader stays reserved for writes that
	// need an authoritative base — the persist branch below (mirrors
	// the aggregateAndWriteStatus cached-read rationale in status.go).
	ledger, err := audit.LoadLedgerForOwner(ctx, r.Client, owner)
	if err != nil {
		log.V(1).Info("relocation directives unavailable: ledger load failed; rendering without node exclusions this pass", "error", err.Error())
		return nil
	}
	if len(ledger.Entries) == 0 {
		return nil
	}
	component := string(ir.Spec.Component)

	pruneReadyInstances := func(l *audit.Ledger) bool {
		pruned := false
		for i := range ir.Status.InstanceStatuses {
			s := &ir.Status.InstanceStatuses[i]
			if s.Phase != v1beta1.OMENativeInstanceReady || s.Operation != nil {
				continue
			}
			if audit.RemoveAutoRecoverEntries(l, component, s.Index) {
				pruned = true
			}
		}
		return pruned
	}
	if pruneReadyInstances(ledger) {
		// Persist branch: re-load LIVE and re-prune before writing —
		// sibling IRs of the same parent write this ledger concurrently,
		// and the persist writes the snapshot wholesale, so a cache-lagged
		// base would drop their rows.
		if live, lerr := audit.LoadLedgerForOwner(ctx, r.APIReader, owner); lerr != nil {
			// Projection below uses the cache-pruned in-memory view; the
			// persisted prune retries next pass.
			log.V(1).Info("relocation-directive prune: live ledger re-load failed; will retry next pass", "error", lerr.Error())
		} else {
			if pruneReadyInstances(live) {
				if perr := audit.PersistLedgerForOwner(ctx, r.Client, owner, gvk, live); perr != nil {
					// Same fallback: project from memory, retry the persist.
					log.V(1).Info("relocation-directive prune persist failed; will retry next pass", "error", perr.Error())
				}
			}
			ledger = live
		}
	}

	if autoMigrateBudget <= 0 {
		return nil
	}
	var out map[int32][]workloadtypes.NodeExclusion
	for i := range ir.Status.InstanceStatuses {
		idx := ir.Status.InstanceStatuses[i].Index
		exclusions := audit.RecentAutoRecoverExclusions(ledger, component, idx, autoMigrateBudget)
		if len(exclusions) == 0 {
			continue
		}
		if out == nil {
			out = map[int32][]workloadtypes.NodeExclusion{}
		}
		out[idx] = exclusions
	}
	return out
}

// stampAutoRelocationSuccess stamps Succeeded=true + CompletedAt on the
// newest un-Succeeded Auto migration record of every instance observed
// Phase=Ready with no in-flight Operation (the same predicate as the
// ledger success-prune). Only the newest record per instance is
// touched — older un-Succeeded records stay as history of relocations
// that never confirmed. The caller treats failures as best-effort because
// the record is a visibility mirror, never a work input.
func (r *Reconciler) stampAutoRelocationSuccess(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	readyIdx := map[int32]bool{}
	for i := range ir.Status.InstanceStatuses {
		s := &ir.Status.InstanceStatuses[i]
		if s.Phase == v1beta1.OMENativeInstanceReady && s.Operation == nil {
			readyIdx[s.Index] = true
		}
	}
	// Steady-state fast path: no candidate record → no apiserver write.
	stampable := false
	for idx := range readyIdx {
		if newestUnsucceededAutoMigration(ir.Status.Migrations, idx) >= 0 {
			stampable = true
			break
		}
	}
	if !stampable {
		return nil
	}

	now := metav1.NewTime(r.now())
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	var committed []v1beta1.MigrationStatus
	wrote := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wrote = false
		fresh := &v1beta1.InferenceReplica{}
		source, err := irstatus.GetDecoded(ctx, r.liveReader(), key, fresh)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return workloadtypes.ErrStatusOwnerGone
			}
			return fmt.Errorf("re-read IR: %w", err)
		}
		if ownerUID == "" || fresh.UID != ownerUID {
			return workloadtypes.ErrStatusOwnerGone
		}
		changed := false
		for idx := range readyIdx {
			pos := newestUnsucceededAutoMigration(fresh.Status.Migrations, idx)
			if pos < 0 {
				continue
			}
			succeeded := true
			completed := now
			fresh.Status.Migrations[pos].Succeeded = &succeeded
			fresh.Status.Migrations[pos].CompletedAt = &completed
			changed = true
		}
		if !changed {
			return nil
		}
		if err := updateInferenceReplicaStatus(ctx, r.statusWriter(), fresh, source); err != nil {
			if apierrors.IsNotFound(err) {
				return workloadtypes.ErrStatusOwnerGone
			}
			return fmt.Errorf("update IR status: %w", err)
		}
		wrote = true
		committed = fresh.Status.Migrations
		return nil
	})
	if err != nil {
		return err
	}
	if wrote {
		out := make([]v1beta1.MigrationStatus, len(committed))
		for i := range committed {
			out[i] = *committed[i].DeepCopy()
		}
		ir.Status.Migrations = out
	}
	return nil
}

// newestUnsucceededAutoMigration returns the slice position of the
// newest (StartedAt; later slice position wins ties) Auto record whose
// SourceInstance is idx and whose Succeeded is not yet true, or -1.
func newestUnsucceededAutoMigration(migrations []v1beta1.MigrationStatus, idx int32) int {
	pos := -1
	for i := range migrations {
		e := &migrations[i]
		if e.Trigger != v1beta1.MigrationTriggerAuto || e.SourceInstance != idx {
			continue
		}
		if e.Succeeded != nil && *e.Succeeded {
			continue
		}
		if pos == -1 || !e.StartedAt.Time.Before(migrations[pos].StartedAt.Time) {
			pos = i
		}
	}
	return pos
}

// EventReasonRollbackRevisionForeign is the Warning raised when
// spec.pacing.rollbackToRevision names a ControllerRevision this replica
// does not control.
const EventReasonRollbackRevisionForeign = "RollbackRevisionForeign"

// rollbackRevision resolves spec.pacing.rollbackToRevision to a
// ControllerRevision this replica controls. A missing revision yields nil
// and the replica rolls to its spec. A revision this replica does not
// control (another controller, or none) is refused the same way, so a
// pacing block written by a user can never render another workload's pod
// template, and the refusal is reported as a Warning.
func (r *Reconciler) rollbackRevision(ctx context.Context, ir *v1beta1.InferenceReplica, eventTarget client.Object) (*appsv1.ControllerRevision, error) {
	if ir.Spec.Pacing == nil || ir.Spec.Pacing.RollbackToRevision == nil || *ir.Spec.Pacing.RollbackToRevision == "" {
		return nil, nil
	}
	name := *ir.Spec.Pacing.RollbackToRevision
	cr := &appsv1.ControllerRevision{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: ir.Namespace, Name: name}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get rollback revision %s: %w", name, err)
	}
	if !metav1.IsControlledBy(cr, ir) {
		if r.Recorder != nil {
			r.Recorder.Eventf(eventTarget, corev1.EventTypeWarning, EventReasonRollbackRevisionForeign,
				"InferenceReplica %s/%s: rollback revision %s is not controlled by this replica; rolling to the spec instead",
				ir.Namespace, ir.Name, name)
		}
		return nil, nil
	}
	return cr, nil
}

// applyRollbackPayload restores revision-owned render inputs. Explicitly
// recorded topology is authoritative. A legacy multi-pod revision has no
// topology field even when its live workers were rendered with OME-generated
// affinity, so a non-empty current topology requires recovery from live pods;
// ambiguity fails closed rather than rendering a split gang. When current
// topology is empty, absence remains a non-blocking topology-free rollback.
func (r *Reconciler) applyRollbackPayload(ctx context.Context, ir *v1beta1.InferenceReplica, desired *workloadtypes.WorkloadDesiredSpec, payload *revision.DataPayload, target *appsv1.ControllerRevision) error {
	if desired == nil || payload == nil {
		return nil
	}
	currentTopology := desired.TopologyKey
	desired.PodSpec = payload.PodSpec
	desired.WorkerPodSpec = payload.WorkerPodSpec
	desired.PodTemplateObjectMeta = payload.PodMeta
	// The width is revision-owned too: a stored revision's gang renders at
	// the worker count it recorded.
	if payload.WorkerSize != nil {
		desired.Runners = workloadtypes.RunnersWithWorkerSize(desired.Runners, *payload.WorkerSize)
	}
	// The pairing protocol is revision-owned: pods rendered from a stored
	// revision must carry that revision's protocol, not the current spec's,
	// or a repaired old-cohort pod would be mislabeled into the new cohort.
	desired.PairingProtocol = ""
	if payload.PairingProtocol != nil {
		desired.PairingProtocol = *payload.PairingProtocol
	}
	desired.TopologyKey = ""
	if payload.TopologyKey != nil {
		desired.TopologyKey = *payload.TopologyKey
		return nil
	}
	if payload.WorkerPodSpec == nil {
		return nil
	}
	if ir == nil || target == nil {
		if currentTopology == "" {
			return nil
		}
		return fmt.Errorf("legacy multi-pod rollback with configured topology requires IR and target revision")
	}
	targetRev := query.RevisionFromName(target.Name)
	if targetRev.Hash() == "" {
		return fmt.Errorf("legacy rollback revision %q has no recognizable revision hash", target.Name)
	}
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	pods, err := query.ListOMENativePodsByName(ctx, r.APIReader, ir.Namespace, ir.NamePrefix(), component, false)
	if err != nil {
		return fmt.Errorf("list live pods for legacy rollback topology recovery: %w", err)
	}
	targetPods := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod != nil && query.RevisionFromPod(pod).Same(targetRev) {
			targetPods = append(targetPods, pod)
		}
	}
	recovered, ok, recoveryErr := workloadpodgroup.GeneratedTopologyKeyFromPods(
		ir.NamePrefix(), component, targetPods)
	if recoveryErr != nil {
		return fmt.Errorf("legacy rollback revision %q has conflicting OME-generated topology: %w", target.Name, recoveryErr)
	}
	if ok {
		desired.TopologyKey = recovered
		return nil
	}
	if currentTopology == "" {
		return nil
	}
	return fmt.Errorf("legacy rollback revision %q has no unambiguous OME-generated topology among %d live target-revision pods", target.Name, len(targetPods))
}

// resolveParent fetches the parent ISVC named on the IR's ParentRef so
// the workload.ReconcileInput.EventTarget can stay anchored to the
// user-visible parent resource. NotFound / Forbidden are best-effort:
// returns nil and the caller falls back to the IR itself. Other errors
// are logged but also non-fatal — events landing on the IR instead of
// the parent is a soft degradation, not a reason to fail the whole
// reconcile.
func (r *Reconciler) resolveParent(ctx context.Context, ir *v1beta1.InferenceReplica) *v1beta1.InferenceService {
	return r.resolveParentFrom(ctx, r.Client, ir)
}

// resolveParentFrom is resolveParent against an explicit reader. The
// teardown path passes the live reader: a stale cached NotFound on a
// still-live parent would re-target the dangling-ledger close at the
// empty IR-owned ledger and leave the parent-owned Started entry to
// resume as a phantom migration.
func (r *Reconciler) resolveParentFrom(ctx context.Context, reads client.Reader, ir *v1beta1.InferenceReplica) *v1beta1.InferenceService {
	parentName := ir.ParentName()
	if parentName == "" {
		return nil
	}
	parent := &v1beta1.InferenceService{}
	key := types.NamespacedName{Namespace: ir.Namespace, Name: parentName}
	if err := reads.Get(ctx, key, parent); err != nil {
		if !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) {
			r.Log.V(1).Info("Failed to fetch parent ISVC for IR event stream; falling back to IR target",
				"ir", client.ObjectKeyFromObject(ir), "parent", key, "err", err)
		}
		return nil
	}
	return parent
}

// irRevisionKey is the per-(name prefix, Component) revision.Key the IR
// path keys ControllerRevisions on. Defined in exactly one place so the
// revision-ensure step (ensureRevisionWithCollisionRetry) and the
// retention sweep (sweepRevisions, retention.go) can never drift apart —
// a mismatched Key would either ensure CRs under one label set and sweep
// under another (retention never fires) or, worse, sweep a foreign
// workload's CRs. Lives here (not retention.go) so reconciler.go keeps
// owning the constants/query imports it already uses.
//
// The key is scoped by ir.NamePrefix() plus the Component, so a projected
// replica's revisions carry its InferenceService's name and a standalone
// replica's carry its own; the labels match what the pod renderer stamps.
func irRevisionKey(ir *v1beta1.InferenceReplica) revision.Key {
	return revision.Key{
		Namespace: ir.Namespace,
		Name:      ir.NamePrefix() + "-" + string(ir.Spec.Component),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: ir.NamePrefix(),
			constants.OMEComponentLabel:           string(ir.Spec.Component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// revisionScopeUID returns the replica's scope UID. The scope UID feeds the
// hash and therefore the revision name; listing, numbering and retention
// select by the key's labels (irRevisionKey). A projected replica's revision
// names and pod revision labels are computed in its InferenceService's UID
// scope; a standalone replica scopes by its own UID. A replica that names a parent but has no
// InferenceService controlling it, or whose controlling InferenceService
// reference has no UID, is refused: it is a projected replica in a broken
// state, not a standalone one.
func revisionScopeUID(ir *v1beta1.InferenceReplica) (types.UID, error) {
	if ref := metav1.GetControllerOf(ir); ref != nil &&
		schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() == isvcGVK.GroupKind() {
		if ref.UID == "" {
			return "", fmt.Errorf("InferenceReplica %s/%s: controlling InferenceService reference has no UID; cannot scope its revision history", ir.Namespace, ir.Name)
		}
		return ref.UID, nil
	}
	if parent := ir.ParentName(); parent != "" {
		return "", fmt.Errorf("InferenceReplica %s/%s names parent %s but no InferenceService controls it; cannot scope its revision history", ir.Namespace, ir.Name, parent)
	}
	if ir.UID == "" {
		return "", fmt.Errorf("InferenceReplica %s/%s has no UID; cannot scope its revision history", ir.Namespace, ir.Name)
	}
	return ir.UID, nil
}

// ensureRevisionWithCollisionRetry computes the target
// ControllerRevision for the IR's current PodSpec, honoring the
// collision contract from workload/revision: on a same-name-different-
// Data (or foreign-ownership) collision, bump
// IR.Status.CollisionCount, persist, and retry the EnsureControllerRevision
// with the new salt. The retry yields a different hash and lands.
func (r *Reconciler) ensureRevisionWithCollisionRetry(ctx context.Context, ir *v1beta1.InferenceReplica, input workloadtypes.ReconcileInput) (*appsv1.ControllerRevision, error) {
	revKey := irRevisionKey(ir)

	// The hash is scoped by revisionScopeUID so two pod sets never share a
	// revision name even when their templates match.
	scopeUID, err := revisionScopeUID(ir)
	if err != nil {
		return nil, err
	}

	hash, raw, twin, err := r.revisionHashAndTwin(ir, input, ir.Status.CollisionCount, scopeUID)
	if err != nil {
		return nil, err
	}
	// A multi-pod revision that records no width stays the target while its
	// gangs run the desired width, so a deploy rolls no gang whose template
	// did not change.
	adopted, err := revision.AdoptRevisionWithoutWidth(ctx, r.Client, r.APIReader, ir, revKey, hash,
		twin, workloadtypes.WorkerSizeOf(input.DesiredSpec.Runners), ir.Status.UpdateRevision,
		r.observedWorkerWidth(ctx, ir, input.ObservedState.InstanceStatuses))
	if err != nil {
		return nil, fmt.Errorf("adopt revision minted without a width: %w", err)
	}
	if adopted != nil {
		return adopted, nil
	}
	target, collision, err := revision.EnsureControllerRevisionFromHash(
		ctx, r.Client, r.APIReader,
		ir, irGVK, revKey, hash, raw,
	)
	if err != nil {
		return nil, err
	}
	if !collision {
		return target, nil
	}

	bumped, berr := r.bumpCollisionCount(ctx, ir)
	if berr != nil {
		return nil, fmt.Errorf("bump CollisionCount after revision collision: %w", berr)
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(ir, corev1.EventTypeWarning, "RevisionCollision",
			"InferenceReplica %s/%s ControllerRevision hash collision; CollisionCount bumped to %d",
			ir.Namespace, ir.Name, utils.DerefInt32(bumped))
	}

	hash, raw, err = r.revisionHash(ir, input, bumped, scopeUID)
	if err != nil {
		return nil, fmt.Errorf("retry after CollisionCount bump: %w", err)
	}
	target, collision, err = revision.EnsureControllerRevisionFromHash(
		ctx, r.Client, r.APIReader,
		ir, irGVK, revKey, hash, raw,
	)
	if err != nil {
		return nil, fmt.Errorf("retry after CollisionCount bump: %w", err)
	}
	if collision {
		return nil, fmt.Errorf("revision still collides after CollisionCount bump (count=%d)", *bumped)
	}
	return target, nil
}

// revisionHash is the memoized canonical revision payload; see revisionHashAndTwin.
func (r *Reconciler) revisionHash(ir *v1beta1.InferenceReplica, input workloadtypes.ReconcileInput, collisionCount *int32, scopeUID types.UID) (string, []byte, error) {
	hash, raw, _, err := r.revisionHashAndTwin(ir, input, collisionCount, scopeUID)
	return hash, raw, err
}

// revisionHashAndTwin memoizes the canonical revision payload and its width-free twin.
// Generation covers spec-derived inputs; the excluded annotation list, collision count, and
// scope UID cover the remaining hash inputs. Cached raw bytes are immutable and only read by
// revision creation.
func (r *Reconciler) revisionHashAndTwin(ir *v1beta1.InferenceReplica, input workloadtypes.ReconcileInput, collisionCount *int32, scopeUID types.UID) (string, []byte, revision.WidthFreeTwin, error) {
	cc := utils.DerefInt32(collisionCount)
	cacheKey := client.ObjectKeyFromObject(ir)
	excludedAnnotationKeys := ir.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey]

	r.revisionHashMu.Lock()
	defer r.revisionHashMu.Unlock()
	// A replica rendered from refs derives its templates from the model and
	// the runtime, which change without a generation bump, so its hash is
	// computed every pass.
	if entry, ok := r.revisionHashCache[cacheKey]; ok && !rendersFromRefs(ir) &&
		entry.uid == ir.UID &&
		entry.generation == ir.Generation &&
		entry.excludedAnnotationKeys == excludedAnnotationKeys &&
		entry.collisionCount == cc &&
		entry.scopeUID == scopeUID {
		return entry.hash, entry.raw, entry.twin, nil
	}

	tmpl := r.revisionTemplate(ir, input)
	hash, raw, err := revision.HashTemplate(tmpl, collisionCount, scopeUID)
	if err != nil {
		return "", nil, revision.WidthFreeTwin{}, err
	}
	twin, _, err := revision.WidthFreeTwinOf(tmpl, collisionCount, scopeUID)
	if err != nil {
		return "", nil, revision.WidthFreeTwin{}, err
	}
	if r.revisionHashCache == nil {
		r.revisionHashCache = make(map[types.NamespacedName]revisionHashEntry)
	}
	r.revisionHashCache[cacheKey] = revisionHashEntry{
		uid:                    ir.UID,
		generation:             ir.Generation,
		excludedAnnotationKeys: excludedAnnotationKeys,
		collisionCount:         cc,
		scopeUID:               scopeUID,
		hash:                   hash,
		raw:                    raw,
		twin:                   twin,
	}
	return hash, raw, twin, nil
}

// revisionTemplate is what the replica's revision hashes: the rendered
// templates, the template metadata without the inherited object annotations
// (which rendered pods keep), the topology key, the pairing protocol and the
// worker count.
func (r *Reconciler) revisionTemplate(ir *v1beta1.InferenceReplica, input workloadtypes.ReconcileInput) revision.Template {
	desired := input.DesiredSpec
	return v1beta1convert.RevisionTemplateOf(v1beta1convert.PodTemplates{
		Primary:    desired.PodSpec,
		Meta:       desired.PodTemplateObjectMeta,
		Worker:     desired.WorkerPodSpec,
		WorkerSize: workloadtypes.WorkerSizeOf(desired.Runners),
	}, desired.TopologyKey, desired.PairingProtocol, hashExcludedAnnotationKeys(ir))
}

// observedWorkerWidth reads the worker count the replica's gangs run on a
// revision, from the cached pods, and only while a row is Ready on it; a
// list that fails observes nothing, which keeps the target where it is.
func (r *Reconciler) observedWorkerWidth(ctx context.Context, ir *v1beta1.InferenceReplica, rows []workloadtypes.InstanceStatus) func(string) (int32, bool) {
	return func(revisionName string) (int32, bool) {
		if !revision.HasReadyRowOn(rows, revisionName) {
			return 0, false
		}
		pods, err := query.ListOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.NamePrefix(), v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), false)
		if err != nil {
			return 0, false
		}
		return revision.ObservedWorkerWidth(rows, pods, revisionName)
	}
}

// hashExcludedAnnotationKeys returns the pod-template annotation keys omitted
// from revision hashing: the inherited InferenceService keys the projector
// stamps on a projected replica and, on a replica rendered from refs, every
// key of the replica's own object annotations, which the render inherits
// into the template the way an InferenceService's are. The refs set is
// derived in memory; nothing is stamped on the replica.
func hashExcludedAnnotationKeys(ir *v1beta1.InferenceReplica) map[string]struct{} {
	excluded := v1beta1convert.ExcludedAnnotationKeys(ir)
	if !rendersFromRefs(ir) || len(ir.Annotations) == 0 {
		return excluded
	}
	if excluded == nil {
		excluded = make(map[string]struct{}, len(ir.Annotations))
	}
	for k := range ir.Annotations {
		excluded[k] = struct{}{}
	}
	return excluded
}

// forgetRevisionHash evicts the memoized revision hash for a deleted IR so
// the cache doesn't retain entries for IRs that no longer exist.
func (r *Reconciler) forgetRevisionHash(key types.NamespacedName) {
	r.revisionHashMu.Lock()
	defer r.revisionHashMu.Unlock()
	delete(r.revisionHashCache, key)
}

func (r *Reconciler) rememberScaleDownSeries(ir *v1beta1.InferenceReplica) {
	if ir == nil {
		return
	}
	key := client.ObjectKeyFromObject(ir)
	identity := scaleDownSeriesIdentity{
		uid:       ir.UID,
		namespace: ir.Namespace,
		isvc:      ir.NamePrefix(),
		component: string(ir.Spec.Component),
	}
	r.scaleDownSeriesMu.Lock()
	previous, found := r.scaleDownSeriesCache[key]
	if r.scaleDownSeriesCache == nil {
		r.scaleDownSeriesCache = make(map[types.NamespacedName]scaleDownSeriesIdentity)
	}
	r.scaleDownSeriesCache[key] = identity
	r.scaleDownSeriesMu.Unlock()
	if found && previous != identity {
		obsmetrics.DeleteScaleDownSeries(previous.namespace, previous.isvc, previous.component)
		obsmetrics.DeleteIRStatusSeries(key.Namespace, key.Name, previous.component)
	}
}

func (r *Reconciler) deleteRememberedScaleDownSeries(key types.NamespacedName) {
	r.scaleDownSeriesMu.Lock()
	identity, found := r.scaleDownSeriesCache[key]
	delete(r.scaleDownSeriesCache, key)
	r.scaleDownSeriesMu.Unlock()
	if found {
		obsmetrics.DeleteScaleDownSeries(identity.namespace, identity.isvc, identity.component)
		obsmetrics.DeleteIRStatusSeries(key.Namespace, key.Name, identity.component)
	}
}

func (r *Reconciler) deleteScaleDownSeries(ir *v1beta1.InferenceReplica) {
	if ir == nil {
		return
	}
	obsmetrics.DeleteScaleDownSeries(ir.Namespace, ir.NamePrefix(), string(ir.Spec.Component))
	obsmetrics.DeleteIRStatusSeries(ir.Namespace, ir.Name, string(ir.Spec.Component))
	key := client.ObjectKeyFromObject(ir)
	r.scaleDownSeriesMu.Lock()
	if identity, found := r.scaleDownSeriesCache[key]; found && identity.uid == ir.UID {
		delete(r.scaleDownSeriesCache, key)
	}
	r.scaleDownSeriesMu.Unlock()
}

// bumpCollisionCount increments IR.Status.CollisionCount (initializing
// to 1 when nil) and persists the status under retry.RetryOnConflict
// so concurrent reconciles can't lose the bump. Each retry re-reads
// the current count and increments from there — collision counting is
// monotonic, so incrementing whatever the apiserver currently shows is
// the right semantic.
//
// The write is scoped to the UID captured at the start of the reconcile.
// Owner disappearance or replacement returns ErrStatusOwnerGone.
func (r *Reconciler) bumpCollisionCount(ctx context.Context, ir *v1beta1.InferenceReplica) (*int32, error) {
	key := client.ObjectKeyFromObject(ir)
	ownerUID := ir.UID
	var next int32
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		source, err := irstatus.GetDecoded(ctx, r.liveReader(), key, fresh)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return workloadtypes.ErrStatusOwnerGone
			}
			return fmt.Errorf("re-read IR for CollisionCount bump: %w", err)
		}
		if ownerUID == "" || fresh.UID != ownerUID {
			return workloadtypes.ErrStatusOwnerGone
		}
		next = int32(1)
		if fresh.Status.CollisionCount != nil {
			next = *fresh.Status.CollisionCount + 1
		}
		fresh.Status.CollisionCount = &next
		if err := updateInferenceReplicaStatus(ctx, r.statusWriter(), fresh, source); err != nil {
			if apierrors.IsNotFound(err) {
				return workloadtypes.ErrStatusOwnerGone
			}
			return fmt.Errorf("persist CollisionCount: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Mirror onto the cached IR so the immediate retry sees the bumped
	// value.
	ir.Status.CollisionCount = &next
	return &next, nil
}
