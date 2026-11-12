package autoscaler

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// defaultDispatchMinReplicas is the compatibility default for omitted bounds.
// Dispatch paths without scale-to-zero support also use it as their floor.
const defaultDispatchMinReplicas int32 = 1

// IRDispatchInput is the per-Component projection of everything
// DispatchAutoscaler needs that's not already on the IR object. The
// ISVC-side component dispatchers (engine.go, decoder.go, router.go)
// build this from their local state and the committed IR returned by
// irprojector.EnsureInferenceReplica.
//
// Alpha API. The shape may change without notice.
type IRDispatchInput struct {
	// Client is the controller-runtime client used to apply the HPA
	// or ScaledObject.
	Client client.Client

	// Scheme is not read: the dispatch sets owner references from the
	// replica. Callers may leave it nil.
	Scheme *runtime.Scheme

	// IR is the committed InferenceReplica returned by
	// irprojector.EnsureInferenceReplica. Its UID is captured into
	// the OwnerReference so GC cascades the HPA / SO when
	// the IR is deleted. Required — DispatchForIRComponent returns
	// an error if IR is nil, terminating, or has an empty UID.
	IR *v1beta1.InferenceReplica

	// ResolvedAutoscaler is the resolver output (the output of
	// ResolveComponentAutoscaler). nil is treated as Class=none.
	ResolvedAutoscaler *v1beta1.ComponentAutoscaler

	// ComponentExt is the per-Component extension spec; min / max
	// replicas come from here. The dispatch does NOT consult any other
	// field — the Autoscaler block is carried on ResolvedAutoscaler above.
	ComponentExt *v1beta1.ComponentExtensionSpec
}

// RawDispatchInput carries the resolved autoscaler and Deployment identity for
// one RawDeployment Component.
type RawDispatchInput struct {
	Client client.Client
	Scheme *runtime.Scheme

	ISVC          *v1beta1.InferenceService
	ComponentMeta metav1.ObjectMeta

	ResolvedAutoscaler *v1beta1.ComponentAutoscaler
	ComponentExt       *v1beta1.ComponentExtensionSpec

	// PolicySourced marks ResolvedAutoscaler as a policy render: dispatch
	// stamps it into the Deployment's last-rendered annotation afterwards,
	// so a later policy outage has a last-known-good block to stand on
	// (RawDeployment has no InferenceReplica to carry one). A pass that is
	// neither policy-sourced nor a hold removes the annotation once its own
	// scaler is in place, so a hold never replays a policy's scaler after
	// OME has replaced it.
	PolicySourced bool

	// PolicyHold is the fail-closed state: ResolvedAutoscaler is nil and
	// dispatch must NOT run the delete-both branch (that would tear down a
	// working scaler because a *policy object* went missing). Instead it
	// re-dispatches the annotation's last-known-good block — bounds keep
	// flowing to the live scaler — or, with no record, does nothing at all:
	// no scaler is ever replaced by a default HPA on a policy failure.
	PolicyHold bool
}

// DispatchForRawComponent targets the Component's Deployment and owner-refs
// the generated autoscaler to the live InferenceService.
func DispatchForRawComponent(ctx context.Context, input RawDispatchInput) error {
	if err := checkRawDispatchInput(input); err != nil {
		return err
	}

	if input.PolicyHold {
		lkg, err := rawLastRendered(ctx, input.Client, input.ComponentMeta.Namespace, input.ComponentMeta.Name)
		if err != nil {
			return err
		}
		if lkg == nil {
			// No record: first reconcile under a broken ref. Dispatch nothing —
			// the Deployment keeps its own replica count and no default HPA
			// appears.
			return nil
		}
		input.ResolvedAutoscaler = lkg
	}

	minR, maxR, err := rawMinMaxReplicas(input.ComponentExt, input.ResolvedAutoscaler)
	if err != nil {
		return fmt.Errorf("DispatchForRawComponent: invalid replica bounds: %w", err)
	}
	owner := *metav1.NewControllerRef(input.ISVC, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
	scaleTargetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: appsv1.SchemeGroupVersion.String(),
		Kind:       "Deployment",
		Name:       input.ComponentMeta.Name,
	}

	dispatchErr := DispatchAutoscaler(ctx, DispatchParams{
		Client:         input.Client,
		Scheme:         input.Scheme,
		Owner:          owner,
		Namespace:      input.ComponentMeta.Namespace,
		Name:           input.ComponentMeta.Name,
		Labels:         cloneMetadataMap(input.ComponentMeta.Labels),
		Annotations:    cloneMetadataMap(input.ComponentMeta.Annotations),
		ScaleTargetRef: scaleTargetRef,
		Autoscaler:     input.ResolvedAutoscaler,
		MinReplicas:    minR,
		MaxReplicas:    maxR,
	})
	// A pending HPA / KEDA switch has already applied this block, so it is
	// recorded as last-known-good before the pending result is returned.
	if dispatchErr != nil && !IsTransitionPending(dispatchErr) {
		return dispatchErr
	}
	if err := recordRawRender(ctx, input, dispatchErr); err != nil {
		return err
	}
	return dispatchErr
}

// checkRawDispatchInput refuses an input that does not name the service and
// the Deployment the autoscaler is written for: a scaler owned by a service
// without a UID, or named for another namespace, could never be collected.
func checkRawDispatchInput(input RawDispatchInput) error {
	if input.ISVC == nil {
		return fmt.Errorf("DispatchForRawComponent: nil ISVC (refusing to stamp orphaned autoscaler)")
	}
	if input.ISVC.Name == "" {
		return fmt.Errorf("DispatchForRawComponent: empty ISVC name")
	}
	if input.ISVC.Namespace == "" {
		return fmt.Errorf("DispatchForRawComponent: empty ISVC namespace")
	}
	if input.ISVC.UID == "" {
		return fmt.Errorf("DispatchForRawComponent: ISVC %s/%s has empty UID (refusing to stamp orphaned autoscaler)", input.ISVC.Namespace, input.ISVC.Name)
	}
	if input.ComponentMeta.Name == "" {
		return fmt.Errorf("DispatchForRawComponent: empty component name")
	}
	if input.ComponentMeta.Namespace == "" {
		return fmt.Errorf("DispatchForRawComponent: empty component namespace")
	}
	if input.ComponentMeta.Namespace != input.ISVC.Namespace {
		return fmt.Errorf("DispatchForRawComponent: namespace mismatch: component %q, ISVC %q", input.ComponentMeta.Namespace, input.ISVC.Namespace)
	}
	return nil
}

// recordRawRender keeps the Deployment's last-known-good record in step with
// the scaler that was just dispatched. A hold changes nothing. A policy's
// render is recorded. A scaler from anywhere else clears the record, so a
// later hold keeps that scaler instead of bringing the old policy's back,
// unless a switch is still pending: the policy's scaler may be all that runs
// until the switch finishes, so a hold in that window must still restore it.
func recordRawRender(ctx context.Context, input RawDispatchInput, dispatchErr error) error {
	switch {
	case input.PolicyHold:
		return nil
	case input.PolicySourced:
		return stampRawLastRendered(ctx, input.Client, input.ComponentMeta.Namespace, input.ComponentMeta.Name, input.ResolvedAutoscaler)
	case dispatchErr != nil:
		return nil
	}
	return clearRawLastRendered(ctx, input.Client, input.ComponentMeta.Namespace, input.ComponentMeta.Name)
}

// rawLastRendered reads the last-known-good policy render off the
// Deployment's annotation. A missing Deployment reads as "no record".
func rawLastRendered(ctx context.Context, c client.Client, namespace, name string) (*v1beta1.ComponentAutoscaler, error) {
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read last-rendered autoscaler for %s/%s: %w", namespace, name, err)
	}
	block, err := UnmarshalLastRendered(deployment.Annotations[constants.AutoscalerPolicyLastRendered])
	if err != nil {
		// A corrupt record is unusable; treat as no record rather than
		// blocking the reconcile forever on undecodable state.
		return nil, nil
	}
	return block, nil
}

// stampRawLastRendered persists the rendered block onto the Deployment so a
// later policy outage fails closed onto it. Patch-only and skipped when
// unchanged, so steady state adds no writes.
func stampRawLastRendered(ctx context.Context, c client.Client, namespace, name string, block *v1beta1.ComponentAutoscaler) error {
	encoded, err := MarshalLastRendered(block)
	if err != nil {
		return err
	}
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("stamp last-rendered autoscaler for %s/%s: %w", namespace, name, err)
	}
	if deployment.Annotations[constants.AutoscalerPolicyLastRendered] == encoded {
		return nil
	}
	patched := deployment.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[constants.AutoscalerPolicyLastRendered] = encoded
	if err := c.Patch(ctx, patched, client.MergeFrom(deployment)); err != nil {
		return fmt.Errorf("stamp last-rendered autoscaler for %s/%s: %w", namespace, name, err)
	}
	return nil
}

// clearRawLastRendered removes the last-known-good policy render from the
// Deployment. Patch-only and skipped when absent, so steady state adds no
// writes.
func clearRawLastRendered(ctx context.Context, c client.Client, namespace, name string) error {
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("clear last-rendered autoscaler for %s/%s: %w", namespace, name, err)
	}
	if _, ok := deployment.Annotations[constants.AutoscalerPolicyLastRendered]; !ok {
		return nil
	}
	patched := deployment.DeepCopy()
	delete(patched.Annotations, constants.AutoscalerPolicyLastRendered)
	if err := c.Patch(ctx, patched, client.MergeFrom(deployment)); err != nil {
		return fmt.Errorf("clear last-rendered autoscaler for %s/%s: %w", namespace, name, err)
	}
	return nil
}

func rawMinMaxReplicas(c *v1beta1.ComponentExtensionSpec, autoscaler *v1beta1.ComponentAutoscaler) (int32, int32, error) {
	if c == nil {
		return 0, 0, fmt.Errorf("component replica bounds are required")
	}
	// Stored objects can reach reconciliation without admission defaults, so
	// omitted bounds use the same compatibility floor as the generators.
	minR := int(defaultDispatchMinReplicas)
	if c.MinReplicas != nil {
		minR = *c.MinReplicas
	}
	if minR < 0 {
		return 0, 0, fmt.Errorf("minReplicas must be non-negative, got %d", minR)
	}
	maxR := c.MaxReplicas
	if maxR < 0 {
		return 0, 0, fmt.Errorf("maxReplicas must be non-negative, got %d", maxR)
	}
	if minR == 0 && !validTypedRawKEDA(autoscaler) {
		return 0, 0, fmt.Errorf("minReplicas=0 requires typed KEDA with at least one trigger")
	}
	if maxR == 0 {
		maxR = minR
		if maxR == 0 {
			maxR = int(defaultDispatchMinReplicas)
		}
	}
	if minR > maxR {
		return 0, 0, fmt.Errorf("minReplicas must not exceed maxReplicas: %d > %d", minR, maxR)
	}
	return int32(minR), int32(maxR), nil
}

func validTypedRawKEDA(autoscaler *v1beta1.ComponentAutoscaler) bool {
	return autoscaler != nil &&
		autoscaler.Class == v1beta1.AutoscalerKEDA &&
		autoscaler.Keda != nil &&
		len(autoscaler.Keda.Triggers) > 0
}

// DispatchForIRComponent is the convenience wrapper the ISVC-side
// component dispatchers use to translate the per-Component
// IRDispatchInput into a full DispatchParams call. Centralized here
// (rather than copy-pasted into engine.go / decoder.go / router.go)
// so the owner-ref shape, ScaleTargetRef GVK, and min/max bounds
// stay in sync across components.
//
// The HPA + ScaledObject are stamped with:
//
//   - Owner: the live IR with Controller=true. Deleting the IR cascades
//     to both via GC.
//   - ScaleTargetRef: ome.io/v1beta1/InferenceReplica/<ir-name>. The
//     IR's /scale subresource is the canonical scale target for
//     IR-managed Components.
//   - Min/Max replicas: input.ComponentExt.MinReplicas (default 1) and
//     input.ComponentExt.MaxReplicas (clamped up to MinReplicas by the
//     IR projection).
//
// Errors are wrapped with the IR namespace/name so the operator log
// reveals which Component failed dispatch.
//
// Alpha API. The signature may change without notice.
func DispatchForIRComponent(ctx context.Context, input IRDispatchInput) error {
	if input.IR == nil {
		return fmt.Errorf("DispatchForIRComponent: nil IR (refusing to stamp orphaned autoscaler)")
	}
	if input.IR.UID == "" {
		return fmt.Errorf("DispatchForIRComponent: IR %s/%s has empty UID (refusing to stamp orphaned autoscaler)", input.IR.Namespace, input.IR.Name)
	}
	if input.IR.DeletionTimestamp != nil {
		return fmt.Errorf("DispatchForIRComponent: IR %s/%s is terminating", input.IR.Namespace, input.IR.Name)
	}

	minR, maxR := minMaxReplicas(input.ComponentExt)

	owner := metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       input.IR.Name,
		UID:        input.IR.UID,
		Controller: ptr.To(true),
	}

	scaleTargetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       input.IR.Name,
	}

	return DispatchAutoscaler(ctx, DispatchParams{
		Client:         input.Client,
		Scheme:         input.Scheme,
		Owner:          owner,
		Namespace:      input.IR.Namespace,
		Name:           input.IR.Name,
		Labels:         cloneMetadataMap(input.IR.Labels),
		Annotations:    irRunnerAnnotations(input.IR),
		ScaleTargetRef: scaleTargetRef,
		Autoscaler:     input.ResolvedAutoscaler,
		MinReplicas:    minR,
		MaxReplicas:    maxR,
	})
}

// irRunnerAnnotations reads effective Component annotations from the first
// fully-rendered Runner template, which is the canonical metadata source for
// Component-level scaler controls.
func irRunnerAnnotations(ir *v1beta1.InferenceReplica) map[string]string {
	if ir == nil || len(ir.Spec.Runners) == 0 {
		return nil
	}
	return cloneMetadataMap(ir.Spec.Runners[0].Template.Annotations)
}

func cloneMetadataMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// minMaxReplicas projects ComponentExtensionSpec bounds into the int32
// pair the generators expect. MinReplicas defaults to 1 when nil or
// non-positive (matches hpa.calculateMinReplicas + keda.defaultMinReplicas).
// MaxReplicas defaults to 0 when zero — the HPA / KEDA generators clamp
// MaxReplicas up to MinReplicas when MaxReplicas < MinReplicas, so a
// zero MaxReplicas degenerates to a single-replica HPA/SO that just
// reports current load without changing the desired count.
func minMaxReplicas(c *v1beta1.ComponentExtensionSpec) (int32, int32) {
	minR := defaultDispatchMinReplicas
	if c != nil && c.MinReplicas != nil && *c.MinReplicas > 0 {
		minR = int32(*c.MinReplicas)
	}
	var maxR int32
	if c != nil {
		maxR = int32(c.MaxReplicas)
	}
	if maxR < minR {
		maxR = minR
	}
	return minR, maxR
}
