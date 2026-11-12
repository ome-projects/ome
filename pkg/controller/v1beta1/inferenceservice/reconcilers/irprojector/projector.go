// Package irprojector projects an InferenceService Component (engine /
// decoder / router) into the per-(ISVC, Component) InferenceReplica
// resource the IR controller reconciles.
//
// The projector is the bridge into the IR-driven world: when a
// Component's effective deployment mode is OMENative, the ISVC
// controller hands the desired per-Component spec to
// ensureInferenceReplica. The IR controller then takes over the
// per-Instance lifecycle — Create / Update / Restart / Migrate /
// Delete — owning its own pods, ControllerRevisions, and per-Component
// headless Service.
//
// The package is intentionally narrow:
//
//   - IsIRManagedComponent reads the resolved deployment mode and
//     returns the predicate the dispatch sites in
//     components/{engine,decoder,router}.go branch on. OMENative-mode
//     Components ALWAYS route through the IR-managed path.
//
//   - EnsureInferenceReplica is the CreateOrUpdate driver: build the
//     desired IR Spec from the rendered per-Component inputs, owner-ref
//     the IR back to the parent ISVC, stamp the controller-write
//     annotation the IR webhook gates on, and persist.
//
// The package does NOT:
//   - reach into the IR controller's internals (writes Spec, reads
//     Status only — see ReadInferenceReplica in status.go);
//   - duplicate pod-spec rendering (the dispatch site already computed
//     the PodSpec; ensure passes it through verbatim);
//   - manage IR deletion beyond the owner-ref cascade (GC handles it
//     on ISVC delete; explicit operator IR deletes are out of scope).
package irprojector

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/render"
)

// IsIRManagedComponent returns true when the given Component should be
// driven through the InferenceReplica path. Only constants.OMENative
// routes through the IR-managed path; every other mode (RawDeployment,
// MultiNode, …) falls through to its own dispatch branch in
// components/{engine,decoder,router}.go.
//
// OMENative-mode Components ALWAYS route through the IR-managed path —
// there is no per-Component opt-in and no cluster-wide opt-out. The IR
// path is the sole OMENative implementation.
func IsIRManagedComponent(deploymentMode constants.DeploymentModeType) bool {
	return deploymentMode == constants.OMENative
}

// isvcGVK identifies the parent ISVC for owner-ref stamping on
// emitted InferenceReplicas. Declared here so the package is
// self-contained and doesn't need a cross-package import.
var isvcGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// HistoryScope is the owner a projected replica's revision history carries
// beside the replica: the InferenceService that controls it, so every
// ControllerRevision the replica mints stays when the replica is deleted
// and projected again, and leaves with the service. nil for a standalone
// replica, whose history is its own.
func HistoryScope(ir *v1beta1.InferenceReplica) *metav1.OwnerReference {
	return revision.ScopeOwner(ir, isvcGVK.GroupKind())
}

// projectedBy reports whether the live replica is the one isvc projects: it
// is controlled by isvc, or it carries isvc as parentRef and no controller
// at all, which is a projected replica whose owner reference was removed
// by hand and is re-stamped below. A replica with no parentRef, or another
// controller, belongs to someone else.
func projectedBy(ir *v1beta1.InferenceReplica, isvc *v1beta1.InferenceService) bool {
	if metav1.IsControlledBy(ir, isvc) {
		return true
	}
	return metav1.GetControllerOf(ir) == nil && ir.ParentName() == isvc.Name
}

// notProjectedError explains why a live replica projectedBy rejects is left
// alone. A replica still controlled by a deleted InferenceService of the
// same name, or one already being deleted, clears on its own, so the
// message says to wait; any other replica holds the name until one of the
// two is renamed.
func notProjectedError(ir *v1beta1.InferenceReplica, isvc *v1beta1.InferenceService) error {
	if ref := metav1.GetControllerOf(ir); ref != nil && ref.Name == isvc.Name && ref.UID != isvc.UID &&
		schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() == isvcGVK.GroupKind() {
		return fmt.Errorf("InferenceReplica %s/%s still belongs to a previous InferenceService %s (uid %s) and is being removed; it is recreated once gone",
			isvc.Namespace, ir.Name, isvc.Name, ref.UID)
	}
	if ir.DeletionTimestamp != nil {
		return fmt.Errorf("InferenceReplica %s/%s (uid %s) is being deleted; it is recreated for InferenceService %s once gone",
			isvc.Namespace, ir.Name, ir.UID, isvc.Name)
	}
	return fmt.Errorf("InferenceReplica %s/%s exists and is not projected by InferenceService %s: it is a standalone replica or another object's; rename one of them",
		isvc.Namespace, ir.Name, isvc.Name)
}

// Params is the input bag EnsureInferenceReplica reads to build the
// desired IR Spec. The dispatch site in
// components/{engine,decoder,router}.go fills it from the rendered
// per-Component values.
//
// Critical contract: PodSpec / WorkerPodSpec / ObjectMeta are the
// rendered per-pod template values. The projector does not re-render —
// it hands the IR controller the exact same per-pod template the
// dispatch site computed.
type Params struct {
	placementExecution    *v1beta1.PlacementExecutionPolicy
	placementReplicaLimit *int32
	// CoordinatorFollower gives the scaling coordinator ownership of this
	// component's desired count. CoordinatorTarget is the count it resolved
	// before this projection, written on create and on update so the count
	// and the partition sized from it land in one write; nil keeps the live
	// count on update and projects the declared minimum on create.
	CoordinatorFollower bool
	CoordinatorTarget   *int32
	// FollowerReleased marks a Component that stopped following the scaling
	// policy while nothing else owns its count: it keeps the count it
	// carries until its minReplicas is edited or a policy names it again.
	FollowerReleased bool

	// ISVC is the parent InferenceService. The projector reads
	// ISVC.Name / Namespace / UID for IR naming + owner-ref, and
	// passes Spec.<component>.Lifecycle into the IR Spec.
	ISVC *v1beta1.InferenceService

	// Component identifies which of engine / decoder / router is
	// being projected. The IR name is <isvc>-<component>; the IR
	// Spec.Component field stamps this verbatim.
	Component v1beta1.ComponentType

	// ComponentExt is the merged ComponentExtensionSpec for this
	// Component. The projector reads MinReplicas (defaults to 1) and
	// Lifecycle. Other fields (autoscaler, KEDA, deployment strategy)
	// are owned by the ISVC controller's other reconcilers — they
	// don't influence the IR Spec.
	ComponentExt *v1beta1.ComponentExtensionSpec

	// ObjectMeta is the rendered per-Component pod metadata. Threaded
	// into the IR via the PodTemplateSpec.ObjectMeta on each Runner so
	// the IR-rendered pods inherit the canonical OMENative labels /
	// annotations.
	ObjectMeta metav1.ObjectMeta

	// PodSpec is the rendered leader / single-pod template. Required.
	PodSpec *corev1.PodSpec

	// WorkerPodSpec is the rendered worker template for multi-pod
	// Components. nil for single-pod Components (router; engine /
	// decoder without Worker block).
	WorkerPodSpec *corev1.PodSpec

	// WorkerSize is Worker.Size for multi-pod Components. Zero for
	// single-pod Components.
	WorkerSize int

	// MultiPod indicates the Component declares Leader + Worker
	// (each Instance materializes more than one pod). Set at the
	// dispatch site since EngineSpec / DecoderSpec aren't carried
	// through this bag.
	MultiPod bool

	// TopologyKey is the resolved gang co-location node-label key for
	// this Component (effective ISVC↔runtime value). Projected verbatim
	// onto ir.Spec.TopologyKey so the IR controller can auto-generate the
	// per-Instance worker→leader podAffinity. Set at the dispatch site
	// since EngineSpec / DecoderSpec aren't carried through this bag. nil
	// for single-pod Components or when unset on both ISVC and runtime.
	TopologyKey *string

	// TopologySpread is the resolved gang spreading policy for the
	// Component, projected verbatim onto ir.Spec.TopologySpread so the IR
	// controller can advertise it on each Instance's PodGroup.
	TopologySpread *v1beta1.TopologySpreadPolicy

	// TopologySpreadKey is the resolved fault-domain label key for
	// TopologySpread; nil defaults to the co-location TopologyKey.
	TopologySpreadKey *string

	// PacingPartition sizes the rollout-control partition the ISVC
	// controller's canary machine projects for this Component (the current
	// step's hold, or the full plan-gate hold) against the replica count
	// this projection stamps, onto ir.Spec.Pacing.Partition. It is kept out
	// of ir.Spec.Lifecycle, which carries the component's update strategy.
	// nil when no canary governs the Component; a resolved 0 releases every
	// Instance for the duration of the plan.
	PacingPartition PartitionResolver

	// RollbackToRevision names the ControllerRevision of the stable side the
	// Component's canary unit keeps: the revision a standing rollback holds
	// the Component on, or the stable revision a live step or a stop holds
	// Instances on. A replica this projection creates is created pointed at
	// it, so its Instances come up on that revision and not on the spec's
	// canary or rejected one; the revision outlives the deleted replica
	// because the service owns it. A replica that already exists keeps the
	// target the canary executor writes on it. Empty when the unit keeps no
	// stable side.
	RollbackToRevision string

	// ResolvedAutoscaler is the authoritative per-Component
	// ComponentAutoscaler the autoscaler.ResolveComponentAutoscaler
	// helper picked from the ISVC → runtime → default chain. The
	// projector deep-copies this onto ir.Spec.Autoscaler with
	// whole-block replace semantics (operator-side ISVC / runtime
	// edits always win over any drifted IR.spec.autoscaler). nil is
	// accepted and projects to nil ir.Spec.Autoscaler (no spurious
	// empty block); callers should normally pass the resolver's
	// non-nil return value.
	ResolvedAutoscaler *v1beta1.ComponentAutoscaler

	// PreserveAutoscaler is the fail-closed hold for the policy layer: the
	// component's autoscalerPolicyRef could not render this pass, and
	// ir.Spec.Autoscaler is the last-known-good store. When true the
	// projector skips the whole-block replace — without the skip, the
	// resolver's fallthrough would overwrite the stored block and the
	// freeze would lose the very state it exists to keep. On IR create
	// there is no stored block, so a held first reconcile projects nil
	// (no scaler, never a default HPA).
	PreserveAutoscaler bool

	// QuotaAcceleratorResources are the resource names whose presence in a pod
	// puts that Component under the quota backend (--accelerator-resources).
	// See quotaGoverned. Empty leaves every Component governed.
	QuotaAcceleratorResources []string

	// Client is the controller-runtime client used to CreateOrUpdate
	// the IR. The ISVC controller already holds this.
	Client client.Client

	// Reader is the live (uncached) reader used to re-read the IR inside
	// the conflict-retry loop. A 409 means the apiserver holds a newer
	// ResourceVersion than the informer has observed, so re-reading the
	// cache resubmits the same stale base and burns the whole backoff.
	// Optional; nil falls back to Client.
	Reader client.Reader
}

// EnsureInferenceReplica computes the desired IR Spec from Params,
// stamps the owner-ref back to the parent ISVC, attaches the
// controller-write annotation the IR webhook gates on, and
// CreateOrUpdates the object. Returns the post-write IR so the
// caller can read IR.Status downstream (status.go uses this).
//
// Idempotent: the second invocation with the same Params produces the
// same Spec and no-ops (projectionUnchanged skips the write). That answer is
// taken from the replica as the cache holds it (cachedCopyAtRest); the live
// object is read only when the cached copy calls for a write, an error or a
// create, and every write is decided on it. On a real change it patches only
// the diffed fields. Placement-managed projections use an optimistic lock so
// concurrent writes cannot regress plan authority. Ordinary local projections
// do not contend with IR status writes. A live replica the ISVC does not
// project (see projectedBy) is left untouched and reported as an error.
//
// Errors are wrapped with the offending IR namespace/name for grep-
// ability in operator logs — except apierrors.IsConflict, which callers
// treat as a benign requeue rather than a hard error.
func EnsureInferenceReplica(ctx context.Context, p Params) (*v1beta1.InferenceReplica, error) {
	p, policy, err := admitProjection(p)
	if err != nil {
		return nil, err
	}
	name := InferenceReplicaName(p.ISVC.Name, p.Component)
	key := types.NamespacedName{Namespace: p.ISVC.Namespace, Name: name}
	logger := log.FromContext(ctx).WithValues(
		"isvc", client.ObjectKey{Namespace: p.ISVC.Namespace, Name: p.ISVC.Name},
		"component", p.Component,
		"inferencereplica", key)

	if atRest := cachedCopyAtRest(ctx, p, key, name, policy); atRest != nil {
		return atRest, nil
	}

	var committed *v1beta1.InferenceReplica
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ir, err := projectOnLive(ctx, p, policy, key, name, logger)
		if err != nil {
			return err
		}
		committed = ir
		return nil
	})
	if err != nil {
		return nil, err
	}
	return committed, nil
}

// admitProjection checks the parameters and the placement policy the service
// carries before anything is read. A missing client, service, extension or
// pod spec, a policy that cannot be decoded, a component minimum that differs
// from the policy's, or a paused placement without a positive minimum is
// refused. It hands back the parameters with the policy and the quota
// governance applied.
func admitProjection(p Params) (Params, *v1beta1.PlacementExecutionPolicy, error) {
	if err := validateParams(p); err != nil {
		return p, nil, err
	}
	policy, err := protocol.FromDerived(p.ISVC)
	if err != nil {
		return p, nil, err
	}
	p.placementExecution = policy
	if policy != nil && len(policy.ReplicaFloors) > 0 {
		if err := protocol.CheckComponentReplicaFloor(policy.ReplicaFloors, p.Component, p.ComponentExt); err != nil {
			return p, nil, err
		}
	}
	if policy != nil && policy.PauseSurge && !positiveComponentMinimum(p.ComponentExt) {
		return p, nil, fmt.Errorf("placement pause requires a resolved positive component floor")
	}
	return applyQuotaGovernance(p), policy, nil
}

// positiveComponentMinimum reports a resolved minimum that is positive and
// fits a 32-bit count.
func positiveComponentMinimum(ext *v1beta1.ComponentExtensionSpec) bool {
	return ext != nil && ext.MinReplicas != nil && *ext.MinReplicas > 0 && int64(*ext.MinReplicas) <= math.MaxInt32
}

// projectOnLive is one attempt on the live replica. A missing replica is
// created. A replica this service does not project, or whose placement
// identity differs from the policy's, is refused and left as it is. Any
// other is patched where the projection changed it.
func projectOnLive(ctx context.Context, p Params, policy *v1beta1.PlacementExecutionPolicy, key types.NamespacedName, name string, logger logr.Logger) (*v1beta1.InferenceReplica, error) {
	reads := client.Reader(p.Client)
	if p.Reader != nil {
		reads = p.Reader
	}
	ir := &v1beta1.InferenceReplica{}
	getErr := reads.Get(ctx, key, ir)
	switch {
	case apierrors.IsNotFound(getErr):
		return createReplica(ctx, p, ir, name, logger)
	case getErr != nil:
		return nil, fmt.Errorf("get IR %s/%s: %w", p.ISVC.Namespace, name, getErr)
	}
	if !projectedBy(ir, p.ISVC) {
		return nil, notProjectedError(ir, p.ISVC)
	}
	if err := protocol.Authorize(ir.Spec.PlacementExecution, policy); err != nil {
		return nil, fmt.Errorf("project IR %s/%s: %w", p.ISVC.Namespace, name, err)
	}
	return patchReplica(ctx, p, policy, ir, name, logger)
}

// createReplica creates the replica the live read found missing. A create
// the apiserver refuses as already existing is a peer's write, or this
// controller's own that the cache had not shown: it is reported as a
// conflict so the pass reads the live replica again, and logged at V(1)
// because a cache that keeps lagging shows up as repeated firings.
func createReplica(ctx context.Context, p Params, missing *v1beta1.InferenceReplica, name string, logger logr.Logger) (*v1beta1.InferenceReplica, error) {
	p.placementReplicaLimit = placementReplicaLimit(missing, p)
	ir := newInferenceReplica(p, name)
	err := p.Client.Create(ctx, ir)
	switch {
	case err == nil:
		return ir, nil
	case apierrors.IsAlreadyExists(err):
		logger.V(1).Info("InferenceReplica racing-create resolved as Conflict; retrying",
			"parentUID", p.ISVC.UID)
		return nil, apierrors.NewConflict(
			schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"},
			name,
			fmt.Errorf("racing create"),
		)
	default:
		return nil, fmt.Errorf("create IR %s/%s: %w", p.ISVC.Namespace, name, err)
	}
}

// patchReplica applies the desired spec on top of the live replica and
// writes the difference as one merge patch. The pacing partition is
// projected; the rest of the pacing block is kept. Nothing is written when
// the projection changed nothing the projector owns, so a steady desired
// spec never raises the replica's version. A placement-managed projection
// locks the patch on the live version so a racing writer is not regressed.
func patchReplica(ctx context.Context, p Params, policy *v1beta1.PlacementExecutionPolicy, ir *v1beta1.InferenceReplica, name string, logger logr.Logger) (*v1beta1.InferenceReplica, error) {
	original := projectionBase(ir)
	p.placementReplicaLimit = placementReplicaLimit(ir, p)
	applyDesiredSpec(ir, p, name)
	if projectionUnchanged(original, ir) {
		return ir, nil
	}
	patch := client.MergeFrom(original)
	if policy != nil {
		patch = client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})
	}
	logProjectionPatch(logger, original, ir, patch)
	if err := p.Client.Patch(ctx, ir, patch); err != nil {
		return nil, fmt.Errorf("patch IR %s/%s: %w", p.ISVC.Namespace, name, err)
	}
	return ir, nil
}

// logProjectionPatch names the fields a write is for. Every write raises
// the replica's generation and a write on every pass starves the readers
// of a fresh generation, so each write must be attributable to a change.
func logProjectionPatch(logger logr.Logger, original, ir *v1beta1.InferenceReplica, patch client.Patch) {
	logKV := []any{"generation", original.Generation}
	if data, dataErr := patch.Data(ir); dataErr == nil {
		logKV = append(logKV, "patch", boundPatchForLog(data))
	} else {
		logKV = append(logKV, "patchRenderError", dataErr.Error())
	}
	logger.V(1).Info("InferenceReplica projection changed; patching", logKV...)
}

// cachedCopyAtRest reads the replica as the cache holds it and returns that
// copy when the projection would write nothing to it: the copy is the one
// this service projects, the placement policy admits it, and the desired
// spec leaves every field the projector owns as it is. Nil for any other
// answer, a missing or unreadable copy included: the live object decides
// then, as it decides every write. A write of this controller the cache has
// not shown yet differs from the copy, so the live object is read; a write
// of another owner the copy does not show yet reaches this controller as a
// replica event, which runs the next pass over it.
func cachedCopyAtRest(ctx context.Context, p Params, key types.NamespacedName, name string, policy *v1beta1.PlacementExecutionPolicy) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{}
	if err := p.Client.Get(ctx, key, ir); err != nil {
		return nil
	}
	if !projectedBy(ir, p.ISVC) || protocol.Authorize(ir.Spec.PlacementExecution, policy) != nil {
		return nil
	}
	base := projectionBase(ir)
	p.placementReplicaLimit = placementReplicaLimit(ir, p)
	applyDesiredSpec(ir, p, name)
	if !projectionUnchanged(base, ir) {
		return nil
	}
	return ir
}

// projectionBase is the object the projection is compared and patched
// against: the metadata and the spec, which the projector rewrites, copied;
// the status, which it never writes, shared. The merge patch is rendered
// from the difference between the base and the projected object, so a
// status one and the same on both sides keeps the rows out of the copy and
// out of the patch.
func projectionBase(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		TypeMeta:   ir.TypeMeta,
		ObjectMeta: *ir.ObjectMeta.DeepCopy(),
		Spec:       *ir.Spec.DeepCopy(),
		Status:     ir.Status,
	}
}

// InferenceReplicaName returns the name of the replica an InferenceService
// projects for a Component: <inferenceservice>-<component>. The replica
// derives its pod, Service and revision names from the InferenceService
// name (its NamePrefix), so this name is a routing identifier that keeps
// kubectl output grouped per Component. The InferenceService controller
// also finds its projected replicas by this name (ComponentIR).
func InferenceReplicaName(isvcName string, component v1beta1.ComponentType) string {
	return isvcName + "-" + string(component)
}

// validateParams enforces the contract the projector relies on. The
// dispatch site in components/{engine,decoder,router}.go always
// passes a non-nil ISVC / ComponentExt / PodSpec; the explicit guard
// surfaces wiring bugs as a clear error instead of a nil deref.
func validateParams(p Params) error {
	if p.Client == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil client")
	}
	if p.ISVC == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil ISVC (component=%s)", p.Component)
	}
	if p.ComponentExt == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil ComponentExtensionSpec (isvc=%s/%s, component=%s)",
			p.ISVC.Namespace, p.ISVC.Name, p.Component)
	}
	if p.PodSpec == nil {
		return fmt.Errorf("EnsureInferenceReplica: nil PodSpec (isvc=%s/%s, component=%s)",
			p.ISVC.Namespace, p.ISVC.Name, p.Component)
	}
	if p.MultiPod && p.WorkerPodSpec == nil {
		return fmt.Errorf("EnsureInferenceReplica: MultiPod=true but nil WorkerPodSpec (isvc=%s/%s, component=%s)",
			p.ISVC.Namespace, p.ISVC.Name, p.Component)
	}
	return nil
}

// newInferenceReplica builds a fresh IR from Params for the Create
// branch. The owner-ref stamps the ISVC as the controller (so
// GC cascades on ISVC delete) and stamps the
// controller-write annotation the IR validating webhook gates on.
//
// Labels are inherited from the rendered per-Component ObjectMeta so
// the IR object carries the same InferenceService, component and
// runtime labels as the Component's pods.
func newInferenceReplica(p Params, name string) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.ISVC.Namespace,
			Labels:    copyMap(p.ObjectMeta.Labels),
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(p.ISVC, isvcGVK),
			},
		},
	}
	applyDesiredSpec(ir, p, name)
	if p.RollbackToRevision != "" {
		if ir.Spec.Pacing == nil {
			ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
		}
		target := p.RollbackToRevision
		ir.Spec.Pacing.RollbackToRevision = &target
	}
	return ir
}

// applyDesiredSpec writes the projected spec onto the replica in named
// steps: the service's claim on the replica, the role, what it runs and how
// many, the rollout settings, the topology, the pacing and pairing a rollout
// asks for, the autoscaler, and last the hash of what the templates became.
// The same steps run on create and on update, so both produce the same spec;
// a step that keeps a live value says so. A step that reads a field another
// step writes comes after it: the partition is sized at the count written in
// this pass, and the hash is taken over the runners, the topology key, the
// pairing protocol and the excluded keys as written.
func applyDesiredSpec(ir *v1beta1.InferenceReplica, p Params, name string) {
	projectOwnership(ir, p)
	projectRole(ir, p)
	projectWorkload(ir, p)
	projectRolloutSettings(ir, p)
	projectTopology(ir, p)
	projectPacingAndPairing(ir, p)
	projectAutoscaler(ir, p)
	projectTemplateHash(ir)
}

// projectOwnership writes the service's claim on the replica: the
// controller-write key admission gates on, the parent generation this
// projection reflects, the inherited annotation keys the revision hash
// leaves out, the Component's labels and the owner reference.
func projectOwnership(ir *v1beta1.InferenceReplica, p Params) {
	// The projector owns its own annotation keys and keeps every other key:
	// operators annotate a projected replica directly (to release a held
	// revision, for one), and the merge patch carries only the keys that
	// changed.
	if ir.Annotations == nil {
		ir.Annotations = map[string]string{}
	}
	ir.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] = constants.InferenceReplicaControllerWriteAnnotationVal
	// The parent generation lands on every pass, including passes that leave
	// the spec untouched, so coordination gates can tell a projection still
	// catching up with a service change from one with nothing to roll. It is
	// object metadata only: it never reaches the pod templates or the
	// revision hash.
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] =
		strconv.FormatInt(p.ISVC.Generation, 10)
	// The inherited service annotation keys the revision hash leaves out.
	// The render keeps the ones outside OME's own and the pod-level prefixes
	// off the pod template, so stripping those before hashing is a no-op.
	if excluded := objectScopedAnnotationKeys(p); len(excluded) > 0 {
		ir.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey] = strings.Join(excluded, ",")
	} else {
		delete(ir.Annotations, constants.RevisionExcludedAnnotationKeysAnnotationKey)
	}
	// The labels are the per-Component ObjectMeta's, replaced wholesale on
	// every pass: a label added by hand is removed on the next reconcile,
	// while annotation keys of another owner are kept.
	ir.Labels = copyMap(p.ObjectMeta.Labels)
	// The owner reference is the live service, so GC cascades the replica on
	// service delete; one edited by hand is replaced on the next reconcile.
	ir.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(p.ISVC, isvcGVK),
	}
}

// projectRole writes which service and role the replica belongs to and the
// placement policy that admits it. ParentRef and Component are immutable
// after create per the replica webhook; on update the same values are
// written again, a no-op for a well-formed replica.
func projectRole(ir *v1beta1.InferenceReplica, p Params) {
	ir.Spec.ParentRef = &v1beta1.ParentReference{
		Name: p.ISVC.Name,
	}
	ir.Spec.PlacementExecution = p.placementExecution.DeepCopy()
	ir.Spec.PlacementReplicaLimit = p.placementReplicaLimit
	ir.Spec.Component = p.Component
}

// projectWorkload writes what the replica runs and how many: the count (see
// desiredReplicas for who owns it on update), the runners rendered from the
// Component's templates, a copy of the lifecycle and minReadySeconds.
func projectWorkload(ir *v1beta1.InferenceReplica, p Params) {
	ir.Spec.Replicas = desiredReplicas(ir, p)
	ir.Spec.Runners = runnersFromParams(p)
	ir.Spec.Lifecycle = lifecycleFromComponentExt(p.ComponentExt)
	ir.Spec.MinReadySeconds = minReadySecondsFromComponentExt(p.ComponentExt)
}

// projectRolloutSettings writes the pause and the revision history limit
// from the service's operator-facing annotations. Admission denies an
// operator's direct spec edits to a projected replica, so the pause follows
// the parent's annotation, and removing the annotation clears the pause on
// every projected replica.
func projectRolloutSettings(ir *v1beta1.InferenceReplica, p Params) {
	paused, fullStop := constants.RolloutPauseState(p.ISVC.Annotations)
	ir.Spec.Paused = paused
	ir.Spec.PauseMode = ""
	if fullStop {
		ir.Spec.PauseMode = v1beta1.PauseModeFreeze
	}
	ir.Spec.RevisionHistoryLimit = revisionHistoryLimitFromISVC(p.ISVC)
}

// projectTopology writes the resolved gang co-location key and the spread
// policy as given. The replica controller reads the key to build the
// worker-to-leader pod affinity of multi-node Components; nil is a no-op
// there.
func projectTopology(ir *v1beta1.InferenceReplica, p Params) {
	ir.Spec.TopologyKey = p.TopologyKey
	ir.Spec.TopologySpread = p.TopologySpread
	ir.Spec.TopologySpreadKey = p.TopologySpreadKey
}

// projectPacingAndPairing writes the pacing block a canary asks for, sized
// at the count written in this pass (see projectedPacing), and the pairing
// protocol on the roles that pair. The protocol rides each pairing role's
// revision hash so a protocol change rolls them all; the router does not
// pair and must not re-roll on a protocol change.
func projectPacingAndPairing(ir *v1beta1.InferenceReplica, p Params) {
	ir.Spec.Pacing = projectedPacing(ir.Spec.Pacing, p.PacingPartition, ir.Spec.Replicas)
	ir.Spec.PairingProtocol = nil
	if role, _ := v1beta1.RoleOf(p.Component); role.Pairs {
		if proto := p.ISVC.Spec.RolloutPairingProtocol(); proto != "" {
			ir.Spec.PairingProtocol = &proto
		}
	}
}

// projectAutoscaler replaces the autoscaler block whole with a copy of the
// resolved one, so an operator's edit to the service or the runtime always
// wins over a drifted block, and a caller mutating the replica cannot
// corrupt the next resolution. A nil resolved block clears the field. Under
// a policy hold the live block is the last known good and is kept as it is.
func projectAutoscaler(ir *v1beta1.InferenceReplica, p Params) {
	if !p.PreserveAutoscaler {
		ir.Spec.Autoscaler = p.ResolvedAutoscaler.DeepCopy()
	}
}

// projectTemplateHash writes the hash of the pod templates in the same
// write as the templates. The replica repeats it in status once its update
// revision was computed from them, so a reader can tell a status written for
// these templates from one that predates them without the generation, which
// also moves on writes the revision leaves out.
func projectTemplateHash(ir *v1beta1.InferenceReplica) {
	if hash := v1beta1convert.TemplateHash(ir, ir.Spec.Runners); hash != "" {
		ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey] = hash
	} else {
		delete(ir.Annotations, constants.InferenceReplicaTemplateHashAnnotationKey)
	}
}

// PartitionResolver sizes the rollout-control partition a canary projects
// for a Component against the replica count the projection stamps, so the
// partition and the count land on the same fleet in one write. nil projects
// no partition.
type PartitionResolver func(replicas int32) *int32

// projectedPacing returns the pacing block to stamp. The rollout-control
// partition is the canary's resolver evaluated at the projected replica
// count and replaces whatever partition the live block carries; it is
// capped at that count — admission rejects a larger value, and holding
// every Instance is the same hold at either number. The rollback target the
// canary executor writes to the same block directly is kept only while a
// canary governs the Component (a resolver was handed in): a Component no
// canary drives follows its own spec, and a target left behind by a
// rollback would hold it on the old revision for good. A block left with no
// fields projects as nil so an IR outside any canary keeps a stable spec.
func projectedPacing(live *v1beta1.InferenceReplicaPacing, canary PartitionResolver, replicas *int32) *v1beta1.InferenceReplicaPacing {
	out := &v1beta1.InferenceReplicaPacing{}
	if live != nil {
		out = live.DeepCopy()
	}
	out.Partition = nil
	if canary == nil {
		out.RollbackToRevision = nil
	} else {
		var n int32
		if replicas != nil {
			n = *replicas
		}
		if partition := canary(n); partition != nil {
			p := *partition
			if replicas != nil && p > *replicas {
				p = *replicas
			}
			out.Partition = &p
		}
	}
	if out.Partition == nil && out.MaxUnavailable == nil && out.RollbackToRevision == nil {
		return nil
	}
	return out
}

// projectionUnchanged reports whether applyDesiredSpec left the fields the
// projector owns (Spec + the stamped metadata) byte-equal to the live
// object — i.e. there is nothing to write. Status is never compared: the
// projector doesn't own it, and the IR controller writes it on a separate
// cadence.
func projectionUnchanged(old, updated *v1beta1.InferenceReplica) bool {
	return equality.Semantic.DeepEqual(old.Spec, updated.Spec) &&
		equality.Semantic.DeepEqual(old.Labels, updated.Labels) &&
		equality.Semantic.DeepEqual(old.Annotations, updated.Annotations) &&
		equality.Semantic.DeepEqual(old.OwnerReferences, updated.OwnerReferences)
}

// maxLoggedPatchBytes bounds the rendered merge patch in the write log. It
// guards log size only — nothing behavioral reads it, and the patch itself is
// always applied in full — so it is a fixed constant rather than a config
// knob.
const maxLoggedPatchBytes = 4096

// boundPatchForLog renders a merge patch for a single log field, cut to
// maxLoggedPatchBytes with an explicit marker so a truncated line is never
// mistaken for the whole patch.
func boundPatchForLog(data []byte) string {
	if len(data) <= maxLoggedPatchBytes {
		return string(data)
	}
	return string(data[:maxLoggedPatchBytes]) + "...(truncated)"
}

// desiredReplicas decides the value to stamp on ir.Spec.Replicas,
// mirroring the Raw path's "preserve existing replicas" approach
// (reconcilers/deployment/deployment_reconciler.go: checkDeploymentExist
// copies existingDeployment.Spec.Replicas onto the target so HPA's writes
// survive the reconcile).
//
// The IR exposes a /scale subresource at .spec.replicas
// (inferencereplica_types.go), so for an autoscaler-managed Component the
// HPA / KEDA / external scaler is the authoritative writer of
// spec.replicas. An accepted zero-floor placement contract preserves explicit
// zero in initial projection and external scale requests. Other components use
// the following rules:
//
//   - CREATE (no live IR yet — ir.ResourceVersion == ""): stamp
//     MinReplicas. There is nothing for the autoscaler to have written
//     yet; MinReplicas is the correct initial desired count regardless of
//     autoscaler class.
//
//   - UPDATE + autoscaler-managed (resolved Class != None): PRESERVE the
//     live ir.Spec.Replicas. Re-stamping MinReplicas would clobber the
//     value HPA / KEDA / an external scaler wrote via /scale, making the
//     ISVC controller and the autoscaler fight over the count every
//     reconcile. Fall back to MinReplicas (at least 1) when the live value
//     is nil or <= 0; a KEDA scaler idling at zero lands here and the
//     replica path runs one Instance.
//
//   - UPDATE + autoscaling OFF (resolved Class == None / nil): Class None
//     means no autoscaler; the InferenceService controller owns the count,
//     so stamp MinReplicas.
//
//   - A scaling follower (CoordinatorFollower) carries the coordinator's
//     count: CoordinatorTarget when it resolved one this pass, on create
//     and on update alike, else the live count it already carries.
//
//   - A Component that stopped following (FollowerReleased) keeps the live
//     count the coordinator left it at; writing MinReplicas again would
//     drop it there in one write with no budget.
//
// ir is the object applyDesiredSpec is mutating — on the Update path it is
// the live IR fetched via Get (so ResourceVersion + the autoscaler-written
// Spec.Replicas are populated); on the Create path it is a freshly built
// object with an empty ResourceVersion.
func desiredReplicas(ir *v1beta1.InferenceReplica, p Params) *int32 {
	creating := ir.ResourceVersion == ""
	// On a policy hold the pass parameter is nil while the IR's stored
	// last-known-good block still drives /scale. Ownership must follow the
	// block that is actually scaling — deciding from the nil parameter would
	// stamp MinReplicas every pass and fight the frozen scaler at HPA
	// cadence, scaling a loaded fleet to min during a policy outage.
	effective := p.ResolvedAutoscaler
	if p.PreserveAutoscaler && !creating {
		effective = ir.Spec.Autoscaler
	}
	// A follower's count is the coordinator's, on create and on update alike;
	// the projection sizes the canary partition against the count written here.
	if p.CoordinatorFollower && p.CoordinatorTarget != nil {
		target := *p.CoordinatorTarget
		return &target
	}
	if protocol.HasZeroReplicaFloor(p.placementExecution, p.Component) {
		if !creating && (isAutoscalerManaged(effective) || p.CoordinatorFollower || p.FollowerReleased) && ir.Spec.Replicas != nil && *ir.Spec.Replicas >= 0 {
			return ir.Spec.Replicas
		}
		floor := int32(*p.ComponentExt.MinReplicas)
		return &floor
	}
	if !creating && (p.CoordinatorFollower || p.FollowerReleased) && ir.Spec.Replicas != nil && *ir.Spec.Replicas > 0 {
		return ir.Spec.Replicas
	}
	if creating || !isAutoscalerManaged(effective) {
		return replicasFromComponentExt(p.ComponentExt)
	}
	// Update + autoscaler-managed: preserve the live (autoscaler-written)
	// value; a live nil or <= 0 falls back to MinReplicas, so a nil is
	// never written into a live scale target.
	if ir.Spec.Replicas != nil && *ir.Spec.Replicas > 0 {
		return ir.Spec.Replicas
	}
	return replicasFromComponentExt(p.ComponentExt)
}

// placementReplicaLimit captures committed demand before the pause is
// acknowledged. Subsequent autoscaler increases cannot expand that reservation.
func placementReplicaLimit(ir *v1beta1.InferenceReplica, p Params) *int32 {
	if p.placementExecution == nil || !p.placementExecution.PauseSurge {
		return nil
	}
	floor := int32(*p.ComponentExt.MinReplicas)
	requested := *desiredReplicas(ir, p)
	limit := max(floor, requested)
	if ir.Spec.PlacementExecution != nil && ir.Spec.PlacementExecution.PauseSurge && ir.Spec.PlacementReplicaLimit != nil {
		limit = max(floor, min(*ir.Spec.PlacementReplicaLimit, requested))
	}
	return &limit
}

// isAutoscalerManaged reports whether the resolved Component autoscaler
// drives the IR's /scale subresource — i.e. whether an autoscaler (not the
// ISVC controller) is the authoritative writer of spec.replicas.
//
// Every class except None has an external writer of spec.replicas: HPA and
// KEDA are OME-managed scalers that target the /scale subresource, and
// External is an operator-owned scaler that writes the scale subresource
// directly. Only None means no autoscaler at all; the InferenceService
// controller owns the count. A nil resolved block is treated as None
// (matches autoscaler.autoscalerClass / resolvedClass).
func isAutoscalerManaged(a *v1beta1.ComponentAutoscaler) bool {
	if a == nil {
		return false
	}
	return a.Class != v1beta1.AutoscalerNone
}

// replicasFromComponentExt projects ComponentExtensionSpec.MinReplicas
// into the IR Spec.Replicas pointer. A nil or non-positive MinReplicas
// projects the API default (constants.DefaultMinReplicas); the admission
// rule that sizes a maintainRatio member reads an unset count the same way.
func replicasFromComponentExt(c *v1beta1.ComponentExtensionSpec) *int32 {
	if c == nil || c.MinReplicas == nil || *c.MinReplicas <= 0 {
		one := int32(constants.DefaultMinReplicas)
		return &one
	}
	r := int32(*c.MinReplicas)
	return &r
}

// runnersFromParams produces the IR.Spec.Runners projection: the rendered
// per-Component templates as the runner list the IR controller reads back
// without re-rendering.
func runnersFromParams(p Params) []v1beta1.Runner {
	return render.Runners(render.Templates{
		ObjectMeta: p.ObjectMeta,
		Primary:    p.PodSpec,
		Worker:     p.WorkerPodSpec,
		WorkerSize: p.WorkerSize,
		MultiPod:   p.MultiPod,
	}, p.ComponentExt)
}

// objectScopedAnnotationKeys returns sorted ISVC annotation keys that were not declared
// on the component and therefore do not define pod revision identity.
func objectScopedAnnotationKeys(p Params) []string {
	if len(p.ISVC.Annotations) == 0 {
		return nil
	}
	var declared map[string]string
	if p.ComponentExt != nil {
		declared = p.ComponentExt.Annotations
	}
	keys := make([]string, 0, len(p.ISVC.Annotations))
	for k := range p.ISVC.Annotations {
		if _, ok := declared[k]; ok {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return keys
}

// revisionHistoryLimitFromISVC projects the operator-facing
// ome.io/revision-history-limit ISVC annotation onto the IR Spec. The
// admission webhook rejects non-integer and non-positive values, so a
// malformed value here (written before the webhook was active, or
// bypassing it) projects as nil — the IR controller then falls back to
// the operator-level config default instead of honoring a bad value.
func revisionHistoryLimitFromISVC(isvc *v1beta1.InferenceService) *int32 {
	raw, ok := isvc.Annotations[constants.RevisionHistoryLimitAnnotation]
	if !ok {
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > math.MaxInt32 {
		return nil
	}
	v := int32(n)
	return &v
}

// lifecycleFromComponentExt deep-copies the LifecycleSpec when set
// (defensive — the ISVC and IR controllers should not share a
// pointer). nil when ComponentExt has no Lifecycle block.
func lifecycleFromComponentExt(c *v1beta1.ComponentExtensionSpec) *v1beta1.LifecycleSpec {
	if c == nil || c.Lifecycle == nil {
		return nil
	}
	return c.Lifecycle.DeepCopy()
}

// minReadySecondsFromComponentExt projects lifecycle.minReadySeconds onto
// the IR's top-level field the workload engine reads. Unset projects to 0
// (Available as soon as Ready).
func minReadySecondsFromComponentExt(c *v1beta1.ComponentExtensionSpec) int32 {
	if c == nil || c.Lifecycle == nil || c.Lifecycle.MinReadySeconds == nil {
		return 0
	}
	return *c.Lifecycle.MinReadySeconds
}

// copyMap returns a shallow copy of m. nil input returns nil so the
// caller can distinguish "no labels" from "empty labels" (some
// downstream serializers care about the distinction).
func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
