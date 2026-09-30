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

	// PacingPartition is the rollout-control partition the ISVC
	// controller's canary machine computed for this Component (the
	// current step's hold, or the full plan-gate hold), projected onto
	// ir.Spec.Pacing.Partition. It is kept out of ir.Spec.Lifecycle,
	// which carries the component's update strategy. nil when no canary
	// governs the Component; an explicit 0 releases every Instance for the
	// duration of the plan.
	PacingPartition *int32

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
// same Spec and no-ops (projectionUnchanged skips the write). On a real
// change it patches only the diffed fields. Placement-managed projections
// use an optimistic lock so concurrent writes cannot regress plan authority.
// Ordinary local projections do not contend with IR status writes. A live
// replica the ISVC does not project (see projectedBy) is left untouched and
// reported as an error.
//
// Errors are wrapped with the offending IR namespace/name for grep-
// ability in operator logs — except apierrors.IsConflict, which callers
// treat as a benign requeue rather than a hard error.
func EnsureInferenceReplica(ctx context.Context, p Params) (*v1beta1.InferenceReplica, error) {
	if err := validateParams(p); err != nil {
		return nil, err
	}
	policy, err := protocol.FromDerived(p.ISVC)
	if err != nil {
		return nil, err
	}
	p.placementExecution = policy
	if policy != nil && len(policy.ReplicaFloors) > 0 {
		if err := protocol.CheckComponentReplicaFloor(policy.ReplicaFloors, p.Component, p.ComponentExt); err != nil {
			return nil, err
		}
	}
	if policy != nil && policy.PauseSurge && (p.ComponentExt == nil || p.ComponentExt.MinReplicas == nil || *p.ComponentExt.MinReplicas <= 0 || int64(*p.ComponentExt.MinReplicas) > math.MaxInt32) {
		return nil, fmt.Errorf("placement pause requires a resolved positive component floor")
	}
	p = applyQuotaGovernance(p)

	name := InferenceReplicaName(p.ISVC.Name, p.Component)
	key := types.NamespacedName{Namespace: p.ISVC.Namespace, Name: name}

	logger := log.FromContext(ctx).WithValues(
		"isvc", client.ObjectKey{Namespace: p.ISVC.Namespace, Name: p.ISVC.Name},
		"component", p.Component,
		"inferencereplica", key)

	var committed *v1beta1.InferenceReplica
	reads := client.Reader(p.Client)
	if p.Reader != nil {
		reads = p.Reader
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ir := &v1beta1.InferenceReplica{}
		getErr := reads.Get(ctx, key, ir)
		switch {
		case apierrors.IsNotFound(getErr):
			p.placementReplicaLimit = placementReplicaLimit(ir, p)
			ir = newInferenceReplica(p, name)
			if err := p.Client.Create(ctx, ir); err != nil {
				if apierrors.IsAlreadyExists(err) {
					// Cache lag: the Get said NotFound but the
					// apiserver already has the IR (the projector's
					// own previous Create just landed, or a racing
					// peer wrote it). Log so repeated firings of this
					// branch — a real symptom of cache desync — surface
					// in operator logs at V(1).
					logger.V(1).Info("InferenceReplica racing-create resolved as Conflict; retrying",
						"parentUID", p.ISVC.UID)
					return apierrors.NewConflict(
						schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"},
						name,
						fmt.Errorf("racing create"),
					)
				}
				return fmt.Errorf("create IR %s/%s: %w", p.ISVC.Namespace, name, err)
			}
			committed = ir
			return nil
		case getErr != nil:
			return fmt.Errorf("get IR %s/%s: %w", p.ISVC.Namespace, name, getErr)
		}
		if !projectedBy(ir, p.ISVC) {
			return notProjectedError(ir, p.ISVC)
		}
		if err := protocol.Authorize(ir.Spec.PlacementExecution, policy); err != nil {
			return fmt.Errorf("project IR %s/%s: %w", p.ISVC.Namespace, name, err)
		}

		// IR exists - apply the desired spec on top of the live object. The
		// pacing partition is projected; the rest of the pacing block (the
		// canary executor's rollback target) is preserved. Paused is projected
		// from the parent ISVC's operator-facing rollout-paused annotation below.
		original := ir.DeepCopy()
		p.placementReplicaLimit = placementReplicaLimit(ir, p)
		applyDesiredSpec(ir, p, name)

		// No-op guard: skip the write entirely when nothing the projector
		// owns changed. In steady state — including a workload stuck in
		// CrashLoopBackOff — the desired spec is constant, so an
		// unconditional write would churn the IR's ResourceVersion every
		// reconcile and fight the IR controller's rapid status writes,
		// producing a conflict hot-loop.
		if projectionUnchanged(original, ir) {
			committed = ir
			return nil
		}

		patch := client.MergeFrom(original)
		if policy != nil {
			patch = client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})
		}

		// Every write bumps the IR's generation, and a write on every pass
		// starves each fresh-snapshot consumer downstream — ObservedGeneration
		// never catches Generation. So a write must be attributable to a
		// field: log the patch that justifies it. Only on the changed path;
		// the no-op guard above returns before this point.
		logKV := []any{"generation", original.Generation}
		if data, dataErr := patch.Data(ir); dataErr == nil {
			logKV = append(logKV, "patch", boundPatchForLog(data))
		} else {
			logKV = append(logKV, "patchRenderError", dataErr.Error())
		}
		logger.V(1).Info("InferenceReplica projection changed; patching", logKV...)

		if err := p.Client.Patch(ctx, ir, patch); err != nil {
			return fmt.Errorf("patch IR %s/%s: %w", p.ISVC.Namespace, name, err)
		}
		committed = ir
		return nil
	})
	if err != nil {
		return nil, err
	}
	return committed, nil
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
	return ir
}

// applyDesiredSpec stamps the projected fields onto the IR. Split out
// so the Create + Update branches in EnsureInferenceReplica produce
// the same Spec (the only difference between Create and Update is
// whether the apiserver assigns a UID — and Spec.Replicas, which is
// autoscaler-owned on Update; see desiredReplicas).
//
// Projects Spec.Pacing.Partition and preserves the rest of Spec.Pacing on
// Update (see projectedPacing). Spec.Paused follows the parent ISVC's
// operator-facing rollout-paused annotation, because admission denies an
// operator's direct spec edits to a projected replica; removing the
// annotation clears the circuit breaker on every projected replica. A
// standalone replica is the user's and never reaches this function.
//
// Preserves Spec.Replicas on Update when the Component is autoscaler-
// managed — the IR's /scale subresource is the HPA / KEDA / external
// scale target, so the autoscaler is the authoritative writer of
// spec.replicas. Re-stamping MinReplicas every reconcile would clobber
// the autoscaler's value and make the ISVC controller and the autoscaler
// fight over the count. See desiredReplicas for the full decision.
func applyDesiredSpec(ir *v1beta1.InferenceReplica, p Params, name string) {
	// Stamp the annotation keys the projector owns and keep every other
	// key: operators annotate a projected replica directly (to release a
	// held revision, for one), and the merge patch carries only the keys
	// that changed.
	if ir.Annotations == nil {
		ir.Annotations = map[string]string{}
	}
	ir.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] = constants.InferenceReplicaControllerWriteAnnotationVal

	// Stamp the parent ISVC generation this projection reflects. The
	// stamp lands on every projector pass — including passes that leave
	// the Spec untouched — so coordination gates can tell a Component
	// whose projection is still catching up with an ISVC bump apart
	// from one that genuinely has nothing to roll. Object-level
	// annotation only: it never reaches pod templates or the revision
	// hash (revisionHash covers pod-template inputs, not IR metadata).
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] =
		strconv.FormatInt(p.ISVC.Generation, 10)

	// Record inherited ISVC annotation keys so revision hashing can ignore object-only
	// metadata without changing pod metadata propagation.
	if excluded := objectScopedAnnotationKeys(p); len(excluded) > 0 {
		ir.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey] = strings.Join(excluded, ",")
	} else {
		delete(ir.Annotations, constants.RevisionExcludedAnnotationKeysAnnotationKey)
	}

	// Labels track the per-Component ObjectMeta stamped on the
	// Component's pods — copying them onto the IR itself gives
	// kubectl-side filterability without changing the pod label-set the
	// IR controller emits. Unlike annotations, the label set is replaced
	// wholesale on every pass, so a label added by hand is removed on the
	// next reconcile while foreign annotation keys are kept.
	ir.Labels = copyMap(p.ObjectMeta.Labels)

	// Owner-ref must stamp the live ISVC so GC cascades the IR on ISVC
	// delete. On Update the re-stamp applies to a replica projectedBy
	// already accepted: owner refs edited by hand on it are replaced on the
	// next reconcile, intentionally.
	ir.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(p.ISVC, isvcGVK),
	}

	// ParentRef + Component are immutable post-create per the IR
	// webhook. On Update we re-stamp the same values — no-op for a
	// well-formed IR, defense-in-depth if someone manually edited
	// the live object.
	ir.Spec.ParentRef = &v1beta1.ParentReference{
		Name: p.ISVC.Name,
	}
	ir.Spec.PlacementExecution = p.placementExecution.DeepCopy()
	ir.Spec.PlacementReplicaLimit = p.placementReplicaLimit
	ir.Spec.Component = p.Component
	ir.Spec.Replicas = desiredReplicas(ir, p)
	ir.Spec.Runners = runnersFromParams(p)
	ir.Spec.Lifecycle = lifecycleFromComponentExt(p.ComponentExt)
	ir.Spec.MinReadySeconds = minReadySecondsFromComponentExt(p.ComponentExt)
	paused, freeze := constants.RolloutPauseState(p.ISVC.Annotations)
	ir.Spec.Paused = paused
	ir.Spec.PauseMode = ""
	if freeze {
		ir.Spec.PauseMode = v1beta1.PauseModeFreeze
	}
	ir.Spec.RevisionHistoryLimit = revisionHistoryLimitFromISVC(p.ISVC)
	// Project the resolved gang co-location key verbatim. The IR
	// controller reads it to auto-generate the worker→leader podAffinity
	// for multi-node Components; nil is a no-op there.
	ir.Spec.TopologyKey = p.TopologyKey
	ir.Spec.TopologySpread = p.TopologySpread
	ir.Spec.TopologySpreadKey = p.TopologySpreadKey
	ir.Spec.Pacing = projectedPacing(ir.Spec.Pacing, p.PacingPartition, ir.Spec.Replicas)
	// Project the P/D pairing protocol onto the Components that pair. The
	// token rides the engine/decoder revision hash so a protocol change rolls
	// both; the router does not participate in pairing and must not re-roll
	// on a protocol change.
	ir.Spec.PairingProtocol = nil
	if p.Component == v1beta1.EngineComponent || p.Component == v1beta1.DecoderComponent {
		if proto := p.ISVC.Spec.RolloutPairingProtocol(); proto != "" {
			ir.Spec.PairingProtocol = &proto
		}
	}
	// Project the resolved Autoscaler onto the IR. Whole-block replace
	// semantics — operator-side changes to isvc.spec.<comp>.autoscaler
	// always win over any drifted IR.spec.autoscaler. DeepCopy keeps the
	// IR's pointer disjoint from the resolver's return value so a downstream
	// caller mutating the IR cannot corrupt the next reconcile's resolution.
	// nil ResolvedAutoscaler clears ir.Spec.Autoscaler — defensive against a
	// dispatch site that doesn't run the resolver (no spurious empty block
	// lands on the IR) — unless PreserveAutoscaler holds the stored block as
	// the policy layer's last-known-good.
	if !p.PreserveAutoscaler {
		ir.Spec.Autoscaler = p.ResolvedAutoscaler.DeepCopy()
	}
}

// projectedPacing returns the pacing block to stamp: the rollout-control
// partition replaces whatever partition the live block carries, while the
// rollback target the canary executor writes to the same block directly is
// preserved. The partition is capped at the projected replica count —
// admission rejects a larger value, and holding every Instance is the same
// hold at either number. A block left with no fields projects as nil so an
// IR outside any canary keeps a stable spec.
func projectedPacing(live *v1beta1.InferenceReplicaPacing, partition *int32, replicas *int32) *v1beta1.InferenceReplicaPacing {
	out := &v1beta1.InferenceReplicaPacing{}
	if live != nil {
		out = live.DeepCopy()
	}
	out.Partition = nil
	if partition != nil {
		p := *partition
		if replicas != nil && p > *replicas {
			p = *replicas
		}
		out.Partition = &p
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
	if protocol.HasZeroReplicaFloor(p.placementExecution, p.Component) {
		if !creating && isAutoscalerManaged(effective) && ir.Spec.Replicas != nil && *ir.Spec.Replicas >= 0 {
			return ir.Spec.Replicas
		}
		floor := int32(*p.ComponentExt.MinReplicas)
		return &floor
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
// into the IR Spec.Replicas pointer. Defaults to 1 when MinReplicas
// is nil OR <= 0 — matches the workload-side projection
// (core/params.go: replicasFromComponentExt) so the rendered Instance
// count is consistent regardless of which adapter built the IR.
func replicasFromComponentExt(c *v1beta1.ComponentExtensionSpec) *int32 {
	if c == nil || c.MinReplicas == nil || *c.MinReplicas <= 0 {
		one := int32(1)
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
