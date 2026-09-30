package autoscaler

import (
	"context"
	"fmt"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/hpa"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/keda"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/scalermetadata"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// DispatchParams is the input bag DispatchAutoscaler reads to emit a
// per-Component HPA or ScaledObject. Callers provide the resolved autoscaler,
// expected controller owner, scale target, and replica bounds.
//
// Alpha API. The shape may change without notice.
type DispatchParams struct {
	// Client is the controller-runtime client used to Get / Create /
	// Update / Delete the HPA + ScaledObject.
	Client client.Client

	// Scheme is the controller-runtime scheme threaded for parity with
	// the legacy reconciler signatures. DispatchAutoscaler uses Owner
	// directly for owner-ref stamping rather than going through
	// controllerutil.SetControllerReference.
	Scheme *runtime.Scheme

	// Owner is the OwnerReference stamped on every emitted HPA /
	// ScaledObject. For the IR-managed path it points at the
	// InferenceReplica so GC cascades the autoscaler when
	// the IR is deleted. Required — DispatchAutoscaler returns an
	// error if Owner.UID is empty (defends against a caller wiring
	// an owner-ref before the IR is committed).
	Owner metav1.OwnerReference

	// Namespace + Name identify the (HPA, ScaledObject) pair the
	// dispatch reconciles. Both objects share the same Name for cross-
	// class delete to be idempotent (switching keda → hpa with the
	// same Name deletes the SO via the same key DispatchAutoscaler
	// would have used to look it up).
	Namespace string
	Name      string

	// Labels + Annotations are the rendered Component metadata propagated to
	// the generated scaler. The stable ISVC + Component labels also identify
	// an OME-managed scaler from the other deployment mode during handoff.
	Labels      map[string]string
	Annotations map[string]string

	// ScaleTargetRef is forwarded verbatim to the generated HPA /
	// ScaledObject. For the IR-managed path callers supply
	// `{ome.io/v1beta1, InferenceReplica, <ir-name>}` so the HPA /
	// ScaledObject targets the IR's /scale subresource.
	ScaleTargetRef autoscalingv2.CrossVersionObjectReference

	// Autoscaler is the resolved ComponentAutoscaler block — the output
	// of autoscaler.ResolveComponentAutoscaler. nil is treated as
	// Class=none (every OME-managed HPA / SO for this (Namespace, Name)
	// is deleted). NOTE: none + external are status-field twins and
	// share the same reconciliation path.
	Autoscaler *v1beta1.ComponentAutoscaler

	// MinReplicas + MaxReplicas are forwarded to the generated HPA /
	// ScaledObject.
	MinReplicas int32
	MaxReplicas int32
}

// DispatchAutoscaler reconciles one Component autoscaler from the resolved
// ComponentAutoscaler block. Deep-equal-on-Get inside the per-class reconciler
// keeps steady-state reconciliation to a no-op.
//
// Three-way dispatch on Autoscaler.Class (NOTE: none + external are
// status-field twins, both fall through to the "delete both" branch — the
// status writer distinguishes them via ManagedBy):
//
//   - hpa  → ensure the HPA exists; delete the OME-managed ScaledObject.
//   - keda → ensure the ScaledObject exists; delete the OME-managed HPA.
//   - none | external | nil → delete OME-managed HPA + ScaledObject.
//
// Managed classes preflight both canonical objects before mutation. A foreign
// or ownerless object holds reconciliation with an error; none and external
// leave such objects untouched. The current controller UID is authoritative.
// A scaler from the opposite OME deployment mode is recognized only when the
// live InferenceReplica verifies the controller bridge and any stable ISVC /
// Component labels match, allowing the owner and scale target to converge.
// Requested-class reconciliation completes before stale sibling deletion so a
// failed class switch preserves the working scaler.
// KEDA dispatch also requires at least one trigger before ownership preflight
// or mutation so an invalid configuration cannot remove a working scaler.
//
// Errors are wrapped with the offending object kind and key for operator logs.
//
// Alpha API. The signature may change without notice.
func DispatchAutoscaler(ctx context.Context, p DispatchParams) error {
	if p.Client == nil {
		return fmt.Errorf("DispatchAutoscaler: nil client")
	}
	if p.Owner.UID == "" {
		return fmt.Errorf("DispatchAutoscaler: empty Owner.UID (refusing to stamp orphaned autoscaler for %s/%s)", p.Namespace, p.Name)
	}
	if p.Namespace == "" || p.Name == "" {
		return fmt.Errorf("DispatchAutoscaler: empty namespace or name (namespace=%q, name=%q)", p.Namespace, p.Name)
	}

	class := autoscalerClass(p.Autoscaler)
	if class == v1beta1.AutoscalerKEDA && (p.Autoscaler.Keda == nil || len(p.Autoscaler.Keda.Triggers) == 0) {
		return fmt.Errorf("DispatchAutoscaler: KEDA autoscaler requires at least one trigger (ns=%s, name=%s)", p.Namespace, p.Name)
	}
	if class == v1beta1.AutoscalerKEDA && p.Autoscaler.Keda.Advanced != nil {
		hpaConfig := p.Autoscaler.Keda.Advanced.HorizontalPodAutoscalerConfig
		if hpaConfig != nil && hpaConfig.Name == p.Name {
			return fmt.Errorf("DispatchAutoscaler: KEDA horizontalPodAutoscalerConfig.name %q conflicts with the reserved OME HPA name (ns=%s, name=%s)", hpaConfig.Name, p.Namespace, p.Name)
		}
	}

	switch class {
	case v1beta1.AutoscalerHPA:
		if err := preflightHPAOwnership(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels); err != nil {
			return fmt.Errorf("validate HPA ownership for HPA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		if err := preflightScaledObjectOwnership(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels); err != nil {
			return fmt.Errorf("validate ScaledObject ownership for HPA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		if err := convergeHPAControllerOwnership(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels); err != nil {
			return fmt.Errorf("converge HPA ownership for HPA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		if err := ensureHPA(ctx, p); err != nil {
			return err
		}
		if err := deleteScaledObjectIfExists(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels, rejectForeignScaler); err != nil {
			return fmt.Errorf("delete stale ScaledObject for HPA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		return nil

	case v1beta1.AutoscalerKEDA:
		if err := preflightScaledObjectOwnership(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels); err != nil {
			return fmt.Errorf("validate ScaledObject ownership for KEDA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		if err := preflightHPAOwnership(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels); err != nil {
			return fmt.Errorf("validate HPA ownership for KEDA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		if err := convergeScaledObjectControllerOwnership(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels); err != nil {
			return fmt.Errorf("converge ScaledObject ownership for KEDA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		if err := ensureScaledObject(ctx, p); err != nil {
			return err
		}
		if err := deleteHPAIfExists(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels, rejectForeignScaler); err != nil {
			return fmt.Errorf("delete stale HPA for KEDA dispatch (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
		}
		return nil

	default:
		// AutoscalerNone, AutoscalerExternal, and nil all converge here.
		// Delete any OME-managed HPA + SO; the status writer is responsible
		// for the surface-level distinction between "none" (no autoscaler at
		// all) and "external" (operator-managed autoscaler).
		if err := deleteHPAIfExists(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels, preserveForeignScaler); err != nil {
			return fmt.Errorf("delete OME-managed HPA on class=%s (ns=%s, name=%s): %w", class, p.Namespace, p.Name, err)
		}
		if err := deleteScaledObjectIfExists(ctx, p.Client, p.Namespace, p.Name, p.Owner, p.Labels, preserveForeignScaler); err != nil {
			return fmt.Errorf("delete OME-managed ScaledObject on class=%s (ns=%s, name=%s): %w", class, p.Namespace, p.Name, err)
		}
		return nil
	}
}

// autoscalerClass extracts the resolved Class, treating nil as none. NOTE:
// external + none fold into the same dispatch branch (both => delete both);
// we keep them as distinct AutoscalerClass values so the status writer wired
// off the same resolved block can still emit the correct ManagedBy.
func autoscalerClass(a *v1beta1.ComponentAutoscaler) v1beta1.AutoscalerClass {
	if a == nil {
		return v1beta1.AutoscalerNone
	}
	return a.Class
}

// ensureHPA stamps the expected owner-ref on the generator output and runs
// the HPA reconciler. The HPAReconciler.Reconcile() loop is idempotent
// (Get-then-diff) so re-running on a steady state is a cheap no-op.
func ensureHPA(ctx context.Context, p DispatchParams) error {
	componentMeta := buildComponentMeta(p)

	var hpaSpec *v1beta1.HPAAutoscaler
	if p.Autoscaler != nil {
		hpaSpec = p.Autoscaler.HPA
	}

	r := hpa.NewHPAReconcilerForTarget(
		p.Client,
		p.Scheme,
		componentMeta,
		p.ScaleTargetRef,
		hpaSpec,
		p.MinReplicas,
		p.MaxReplicas,
	)
	if err := r.Reconcile(); err != nil {
		return fmt.Errorf("HPA reconcile (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
	}
	_ = ctx // reconciler currently uses context.TODO internally; threaded here for future ctx-aware refactor
	return nil
}

// ensureScaledObject stamps the expected owner-ref on the generator output
// and runs the KEDA reconciler. The KEDAReconciler.Reconcile() loop is
// idempotent (Get-then-diff).
func ensureScaledObject(ctx context.Context, p DispatchParams) error {
	componentMeta := buildComponentMeta(p)

	var kedaSpec *v1beta1.KedaAutoscaler
	if p.Autoscaler != nil {
		kedaSpec = p.Autoscaler.Keda
	}

	r := keda.NewKEDAReconcilerForTarget(
		p.Client,
		p.Scheme,
		componentMeta,
		kedaScaleTarget(p.ScaleTargetRef),
		kedaSpec,
		p.MinReplicas,
		p.MaxReplicas,
	)
	if err := r.Reconcile(); err != nil {
		return fmt.Errorf("ScaledObject reconcile (ns=%s, name=%s): %w", p.Namespace, p.Name, err)
	}
	_ = ctx
	return nil
}

// buildComponentMeta assembles the ObjectMeta the HPA / KEDA generators
// consume. Owner-ref MUST be set here (the generators write it onto the
// generated object directly — they don't call SetControllerReference).
// The caller-provided labels and annotations are copied so generator-side
// mutations cannot alias the rendered Component metadata. When the legacy
// autoscalerClass annotation is present, its emitted value follows the
// resolved typed class so the shared dispatch decision remains authoritative.
func buildComponentMeta(p DispatchParams) metav1.ObjectMeta {
	annotations := cloneMetadataMap(p.Annotations)
	if _, present := annotations[constants.AutoscalerClass]; present {
		switch autoscalerClass(p.Autoscaler) {
		case v1beta1.AutoscalerHPA:
			annotations[constants.AutoscalerClass] = string(constants.AutoscalerClassHPA)
		case v1beta1.AutoscalerKEDA:
			annotations[constants.AutoscalerClass] = string(constants.AutoscalerClassKEDA)
		}
	}
	labels, annotations := scalermetadata.Track(p.Labels, annotations)
	return metav1.ObjectMeta{
		Name:            p.Name,
		Namespace:       p.Namespace,
		Labels:          labels,
		Annotations:     annotations,
		OwnerReferences: []metav1.OwnerReference{p.Owner},
	}
}

// kedaScaleTarget converts the autoscalingv2 scaleTargetRef shape (which
// the dispatch params expose to keep both branches symmetric) into the
// kedav1.ScaleTarget the KEDA generator expects. APIVersion / Kind /
// Name flow through 1:1.
func kedaScaleTarget(ref autoscalingv2.CrossVersionObjectReference) kedav1.ScaleTarget {
	return kedav1.ScaleTarget{
		APIVersion: ref.APIVersion,
		Kind:       ref.Kind,
		Name:       ref.Name,
	}
}

type foreignScalerPolicy bool

const (
	preserveForeignScaler foreignScalerPolicy = false
	rejectForeignScaler   foreignScalerPolicy = true
)

// deleteHPAIfExists deletes the canonical HPA only when it is controlled by
// the expected owner or a recognized OME owner from the opposite deployment
// mode. The caller decides whether a foreign object is preserved silently or
// reported as a reconciliation conflict.
func deleteHPAIfExists(ctx context.Context, c client.Client, namespace, name string, owner metav1.OwnerReference, labels map[string]string, policy foreignScalerPolicy) error {
	obj, err := getHPAIfExists(ctx, c, namespace, name)
	if err != nil || obj == nil {
		return err
	}
	managed, err := controlledByExpectedOrVerifiedModeBridge(ctx, c, obj, owner, labels)
	if err != nil {
		return err
	}
	if !managed {
		if policy == rejectForeignScaler {
			return requireExpectedController(ctx, c, "HorizontalPodAutoscaler", obj, owner, labels)
		}
		return nil
	}
	uid := obj.UID
	resourceVersion := obj.ResourceVersion
	if err := c.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func getHPAIfExists(ctx context.Context, c client.Client, namespace, name string) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	obj := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return obj, nil
}

// deleteScaledObjectIfExists applies the same ownership policy to the
// canonical name produced by utils.GetScaledObjectName.
func deleteScaledObjectIfExists(ctx context.Context, c client.Client, namespace, name string, owner metav1.OwnerReference, labels map[string]string, policy foreignScalerPolicy) error {
	obj, err := getScaledObjectIfExists(ctx, c, namespace, name)
	if err != nil || obj == nil {
		return err
	}
	managed, err := controlledByExpectedOrVerifiedModeBridge(ctx, c, obj, owner, labels)
	if err != nil {
		return err
	}
	if !managed {
		if policy == rejectForeignScaler {
			return requireExpectedController(ctx, c, "ScaledObject", obj, owner, labels)
		}
		return nil
	}
	uid := obj.UID
	resourceVersion := obj.ResourceVersion
	if err := c.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func getScaledObjectIfExists(ctx context.Context, c client.Client, namespace, name string) (*kedav1.ScaledObject, error) {
	soName := utils.GetScaledObjectName(name)
	obj := &kedav1.ScaledObject{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: soName}, obj); err != nil {
		// Nothing to clean up when the ScaledObject is gone (IsNotFound), or when
		// KEDA is not installed at all — CRD absent yields no REST mapping
		// (IsNoMatchError), and the type may be unregistered in the scheme
		// (IsNotRegisteredError). None of these should fail the reconcile.
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
			return nil, nil
		}
		return nil, err
	}
	return obj, nil
}

func preflightHPAOwnership(ctx context.Context, c client.Client, namespace, name string, owner metav1.OwnerReference, labels map[string]string) error {
	obj, err := getHPAIfExists(ctx, c, namespace, name)
	if err != nil || obj == nil {
		return err
	}
	return requireExpectedController(ctx, c, "HorizontalPodAutoscaler", obj, owner, labels)
}

func preflightScaledObjectOwnership(ctx context.Context, c client.Client, namespace, name string, owner metav1.OwnerReference, labels map[string]string) error {
	obj, err := getScaledObjectIfExists(ctx, c, namespace, name)
	if err != nil || obj == nil {
		return err
	}
	return requireExpectedController(ctx, c, "ScaledObject", obj, owner, labels)
}

func requireExpectedController(ctx context.Context, c client.Client, kind string, obj metav1.Object, owner metav1.OwnerReference, labels map[string]string) error {
	managed, err := controlledByExpectedOrVerifiedModeBridge(ctx, c, obj, owner, labels)
	if err != nil {
		return err
	}
	if managed {
		return nil
	}
	controller := metav1.GetControllerOf(obj)
	if controller == nil {
		return fmt.Errorf("%s %s/%s is not controlled by expected owner %s %s (UID %q): object has no controller owner", kind, obj.GetNamespace(), obj.GetName(), owner.Kind, owner.Name, owner.UID)
	}
	return fmt.Errorf("%s %s/%s is not controlled by expected owner %s %s (UID %q): controller is %s %s (UID %q)", kind, obj.GetNamespace(), obj.GetName(), owner.Kind, owner.Name, owner.UID, controller.Kind, controller.Name, controller.UID)
}

func controlledByExpectedOwner(obj metav1.Object, owner metav1.OwnerReference) bool {
	controller := metav1.GetControllerOf(obj)
	return controller != nil && controller.UID == owner.UID
}

func controlledByExpectedOrVerifiedModeBridge(
	ctx context.Context,
	c client.Client,
	obj metav1.Object,
	owner metav1.OwnerReference,
	labels map[string]string,
) (bool, error) {
	if controlledByExpectedOwner(obj, owner) {
		return true, nil
	}
	return controlledByVerifiedModeBridge(ctx, c, obj, owner, labels)
}

// controlledByVerifiedModeBridge recognizes a scaler from the other OME
// deployment mode only when the live InferenceReplica verifies both controller
// owners and the desired ISVC Component. Stable labels must match when present.
func controlledByVerifiedModeBridge(
	ctx context.Context,
	c client.Client,
	obj metav1.Object,
	owner metav1.OwnerReference,
	labels map[string]string,
) (bool, error) {
	controller := metav1.GetControllerOf(obj)
	if controller == nil || controller.APIVersion != v1beta1.SchemeGroupVersion.String() || controller.Kind == owner.Kind {
		return false, nil
	}
	isvcName := labels[constants.InferenceServicePodLabelKey]
	component := labels[constants.OMEComponentLabel]
	if isvcName == "" || component == "" {
		return false, nil
	}
	objectLabels := obj.GetLabels()
	objectISVC, hasISVC := objectLabels[constants.InferenceServicePodLabelKey]
	objectComponent, hasComponent := objectLabels[constants.OMEComponentLabel]
	if hasISVC || hasComponent {
		if !hasISVC || !hasComponent || objectISVC != isvcName || objectComponent != component {
			return false, nil
		}
	}

	var irName string
	var irUID types.UID
	var isvcUID types.UID
	switch {
	case controller.Kind == "InferenceReplica" && owner.Kind == "InferenceService":
		irName = controller.Name
		irUID = controller.UID
		isvcUID = owner.UID
	case controller.Kind == "InferenceService" && owner.Kind == "InferenceReplica":
		irName = owner.Name
		irUID = owner.UID
		isvcUID = controller.UID
	default:
		return false, nil
	}

	ir := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: irName}, ir); err != nil {
		if apierrors.IsNotFound(err) || runtime.IsNotRegisteredError(err) {
			return false, nil
		}
		return false, err
	}
	irController := metav1.GetControllerOf(ir)
	if ir.UID != irUID || irController == nil ||
		irController.APIVersion != v1beta1.SchemeGroupVersion.String() ||
		irController.Kind != "InferenceService" || irController.UID != isvcUID {
		return false, nil
	}
	return ir.Labels[constants.InferenceServicePodLabelKey] == isvcName &&
		ir.Labels[constants.OMEComponentLabel] == component &&
		ir.ParentName() == isvcName &&
		ir.Spec.Component == v1beta1.ComponentType(component), nil
}

// convergeHPAControllerOwnership transfers a recognized same-class HPA to the
// live mode owner before the per-class reconciler updates metadata and target.
func convergeHPAControllerOwnership(ctx context.Context, c client.Client, namespace, name string, owner metav1.OwnerReference, labels map[string]string) error {
	obj, err := getHPAIfExists(ctx, c, namespace, name)
	if err != nil || obj == nil || controlledByExpectedOwner(obj, owner) {
		return err
	}
	managed, err := controlledByExpectedOrVerifiedModeBridge(ctx, c, obj, owner, labels)
	if err != nil {
		return err
	}
	if !managed {
		return requireExpectedController(ctx, c, "HorizontalPodAutoscaler", obj, owner, labels)
	}
	obj.OwnerReferences = replaceControllerOwner(obj.OwnerReferences, owner)
	return c.Update(ctx, obj)
}

// convergeScaledObjectControllerOwnership is the ScaledObject equivalent of
// convergeHPAControllerOwnership.
func convergeScaledObjectControllerOwnership(ctx context.Context, c client.Client, namespace, name string, owner metav1.OwnerReference, labels map[string]string) error {
	obj, err := getScaledObjectIfExists(ctx, c, namespace, name)
	if err != nil || obj == nil || controlledByExpectedOwner(obj, owner) {
		return err
	}
	managed, err := controlledByExpectedOrVerifiedModeBridge(ctx, c, obj, owner, labels)
	if err != nil {
		return err
	}
	if !managed {
		return requireExpectedController(ctx, c, "ScaledObject", obj, owner, labels)
	}
	obj.OwnerReferences = replaceControllerOwner(obj.OwnerReferences, owner)
	return c.Update(ctx, obj)
}

func replaceControllerOwner(refs []metav1.OwnerReference, owner metav1.OwnerReference) []metav1.OwnerReference {
	updated := make([]metav1.OwnerReference, 0, len(refs)+1)
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			continue
		}
		updated = append(updated, ref)
	}
	return append(updated, owner)
}
