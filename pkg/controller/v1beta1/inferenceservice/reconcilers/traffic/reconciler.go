// Per-InferenceService traffic-management reconciler — top-level entry
// point that resolves the operator's intent, invokes the active
// translator, applies the emitted backend policy resource, and returns
// the TrafficStatus to write back to the ISVC.
//
// Construction lives in package factory; the InferenceServiceReconciler
// holds a *Reconciler and calls Reconcile after the ingress reconciler
// has materialized the OME-managed HTTPRoutes.
package traffic

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/status"
)

// ErrConflictingPolicy is returned by applyPolicy when a backend
// policy resource with the name OME would emit already exists but is
// not owned by the InferenceService being reconciled — either it has
// a different controller, no controller-ref at all (hand-authored),
// or a controller-ref to a different InferenceService. Caller surfaces
// this as BackendPolicyReady=False/ConflictingPolicy in the status and
// must NOT overwrite the pre-existing object.
var ErrConflictingPolicy = errors.New("backend policy with the OME-managed name exists but is not owned by this InferenceService")

// Reconciler turns operator intent into a backend policy resource via
// the active Translator and surfaces a TrafficStatus the caller can
// assign to the InferenceService's status subresource.
type Reconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	translator Translator
	// now lets tests inject a stable timestamp.
	now func() metav1.Time
}

// NewReconciler builds a Reconciler around the active translator.
// translator must be non-nil; callers obtain it from the factory at
// controller startup.
func NewReconciler(c client.Client, scheme *runtime.Scheme, translator Translator) *Reconciler {
	return &Reconciler{
		client:     c,
		scheme:     scheme,
		translator: translator,
		now:        metav1.Now,
	}
}

// TranslatorName returns the active translator's stable identifier.
// Useful for log fields and metrics labels on the caller side.
func (r *Reconciler) TranslatorName() string {
	return r.translator.Name()
}

// Watches returns a zero-value of the resource type the active
// translator emits, or nil when the translator emits nothing. The
// caller's SetupWithManager passes this to .Owns() so policy changes
// re-enqueue the owning ISVC.
func (r *Reconciler) Watches() client.Object {
	return r.translator.Watches()
}

// Reconcile resolves intent, invokes the translator, applies the
// emitted policy resource (if any), and returns the TrafficStatus the
// caller should write back. Returns (nil, nil) when no intent is
// declared — caller should clear or leave isvc.Status.Traffic alone in
// that case (Build is what produces nil; ownership of the prior status
// is the caller's call).
//
// targetHTTPRoutes lists the OME-managed HTTPRoute names the backend
// policy should target. Callers compute this from the ISVC + which
// components are present (Engine always; Decoder/Router conditional).
func (r *Reconciler) Reconcile(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	targetHTTPRoutes []string,
) (*v1beta1.TrafficStatus, error) {
	intent := Resolve(isvc)
	if !intent.HasIntent() {
		return nil, r.cleanupPriorPolicy(ctx, isvc)
	}

	emitted, passthroughs, translateErr := r.translator.Translate(isvc, targetHTTPRoutes, intent)
	written := r.writeEmitted(ctx, isvc, emitted, translateErr)

	out := status.Build(status.BuildArgs{
		TranslatorName:           r.translator.Name(),
		HasIntent:                true,
		Algorithm:                algorithmOf(intent),
		EmittedPolicy:            emitted,
		TargetedRoutes:           targetHTTPRoutes,
		Passthroughs:             passthroughs,
		TranslateErr:             written.err,
		ConflictDetected:         written.conflictMessage != "",
		ConflictMessage:          written.conflictMessage,
		ObservedGeneration:       isvc.Generation,
		Now:                      r.now(),
		GatewayAcceptance:        written.acceptance.State,
		GatewayAcceptanceReason:  written.acceptance.Reason,
		GatewayAcceptanceMessage: written.acceptance.Message,
		UnsupportedAnnotations:   ComputeUnsupportedAnnotations(isvc.Annotations, r.translator),
		UnsupportedFields:        ComputeUnsupportedTrafficFields(intent.Traffic, r.translator),
	})

	return out, written.err
}

// policyWrite is what came of the emitted policy: the error the pass
// reports (the translation error, or the apply error in its place), the
// conflict text when another owner holds the name, and the gateway's
// acceptance of what was written.
type policyWrite struct {
	err             error
	conflictMessage string
	acceptance      AcceptanceObservation
}

// writeEmitted applies the emitted policy when the translator produced
// one without error, then reads the gateway's acceptance of it. Nothing
// is observed after a failed apply: on a conflict the live object is
// another owner's, and otherwise nothing of ours was written. On the
// first pass the gateway has usually not run yet, so the acceptance
// reads Pending until the policy's own watch re-enqueues the ISVC.
func (r *Reconciler) writeEmitted(ctx context.Context, isvc *v1beta1.InferenceService, emitted client.Object, translateErr error) policyWrite {
	out := policyWrite{err: translateErr, acceptance: AcceptanceObservation{State: AcceptancePending}}
	if translateErr != nil || emitted == nil {
		return out
	}
	if applyErr := r.applyPolicy(ctx, isvc, emitted); applyErr != nil {
		out.err = applyErr
		if errors.Is(applyErr, ErrConflictingPolicy) {
			out.conflictMessage = applyErr.Error()
		}
		return out
	}
	out.acceptance = r.observeAcceptance(ctx, emitted)
	return out
}

// cleanupPriorPolicy deletes the previously-emitted backend policy
// resource when the operator has removed all traffic intent. Owner
// references handle ISVC deletion; this handles the "intent removed
// but ISVC remains" case.
//
// Gated on isvc.Status.Traffic.BackendPolicyResource so the common
// case (ISVC never had traffic intent) costs no extra API call. The
// gate has a small leak window — if a previous reconcile applied a
// policy but crashed before writing status, and then the operator
// removes intent, we'd skip the Delete. Operators can recover by
// re-adding intent and removing it again, or by deleting the BTP by
// hand. Trading that edge case for not pestering the API server with
// a no-op Delete on every reconcile of every intent-free ISVC.
func (r *Reconciler) cleanupPriorPolicy(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if isvc.Status.Traffic == nil || isvc.Status.Traffic.BackendPolicyResource == nil {
		return nil
	}
	// The noop translator emits nothing, so there is nothing to delete.
	watch := r.translator.Watches()
	if watch == nil {
		return nil
	}
	// The status carries the name the translator emitted, so a translator
	// with another naming rule is cleaned up by the same path.
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: isvc.Status.Traffic.BackendPolicyResource.Name}
	obj, ok := fetchTarget(watch, key)
	if !ok {
		return fmt.Errorf("translator.Watches() did not produce a client.Object")
	}
	if err := r.client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete prior backend policy %s/%s: %w", isvc.Namespace, obj.GetName(), err)
	}
	return nil
}

// observeAcceptance fetches the freshly-applied policy resource and
// asks the translator to interpret its status. A fetch that fails reads
// as Pending; the next reconcile retries.
func (r *Reconciler) observeAcceptance(ctx context.Context, desired client.Object) AcceptanceObservation {
	key := client.ObjectKeyFromObject(desired)
	fetched, ok := fetchTarget(desired, key)
	if !ok || r.client.Get(ctx, key, fetched) != nil {
		return AcceptanceObservation{State: AcceptancePending}
	}
	return r.translator.ObserveAcceptance(fetched)
}

func algorithmOf(intent *ResolvedIntent) string {
	if intent == nil || intent.Traffic == nil || intent.Traffic.Algorithm == nil {
		return ""
	}
	return string(*intent.Traffic.Algorithm)
}

// applyPolicy sets the controller reference and creates or updates the
// emitted policy resource. A live object of the same name that this
// InferenceService does not own is left alone and reported as
// ErrConflictingPolicy. The translator's output is deterministic, so
// the update is a coarse Update of the whole desired object.
func (r *Reconciler) applyPolicy(ctx context.Context, isvc *v1beta1.InferenceService, desired client.Object) error {
	if err := controllerutil.SetControllerReference(isvc, desired, r.scheme); err != nil {
		return fmt.Errorf("set controller reference on backend policy: %w", err)
	}
	key := client.ObjectKeyFromObject(desired)
	existing, ok := fetchTarget(desired, key)
	if !ok {
		return errors.New("backend policy resource does not implement client.Object")
	}

	err := r.client.Get(ctx, key, existing)
	switch {
	case apierrors.IsNotFound(err):
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("create backend policy %s: %w", key, createErr)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get backend policy %s: %w", key, err)
	case !ownedBy(existing, isvc):
		return fmt.Errorf("%w: %s already exists with %s", ErrConflictingPolicy, key, describeController(existing))
	}

	// The live resource version keeps the optimistic-concurrency token
	// on the Update.
	desired.SetResourceVersion(existing.GetResourceVersion())
	if updateErr := r.client.Update(ctx, desired); updateErr != nil {
		return fmt.Errorf("update backend policy %s: %w", key, updateErr)
	}
	return nil
}

// fetchTarget is an empty object of obj's Go type that carries only
// obj's kind and key's name and namespace, for Get to fill in. It starts
// empty because a typed read straight from the API server keeps every
// field the server did not send: a target copied from the desired object
// would show the desired controller reference on a hand-authored policy,
// or on nothing at all when the read finds no object.
func fetchTarget(obj client.Object, key client.ObjectKey) (client.Object, bool) {
	objType := reflect.TypeOf(obj)
	if objType == nil || objType.Kind() != reflect.Pointer {
		return nil, false
	}
	target, ok := reflect.New(objType.Elem()).Interface().(client.Object)
	if !ok {
		return nil, false
	}
	target.GetObjectKind().SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
	target.SetName(key.Name)
	target.SetNamespace(key.Namespace)
	return target, true
}

// ownedBy reports whether obj's controller owner reference points at
// isvc. Returns false when there is no controller-ref (hand-authored)
// or the controller-ref names a different object.
func ownedBy(obj client.Object, isvc *v1beta1.InferenceService) bool {
	ctrl := metav1.GetControllerOf(obj)
	if ctrl == nil {
		return false
	}
	return ctrl.UID == isvc.UID || (ctrl.Name == isvc.Name && ctrl.Kind == "InferenceService")
}

// describeController returns a human-readable description of obj's
// controller owner for use in the ConflictingPolicy error message.
func describeController(obj client.Object) string {
	ctrl := metav1.GetControllerOf(obj)
	if ctrl == nil {
		return "no controller owner reference (hand-authored)"
	}
	return fmt.Sprintf("controller=%s/%s", ctrl.Kind, ctrl.Name)
}

// ComputeTargetHTTPRoutes returns the OME-managed HTTPRoute names the
// backend policy should target for the given InferenceService. Always
// includes the top-level route (isvc.Name) and the engine route; adds
// the decoder route when hasDecoder is true and the router route when
// hasRouter is true.
//
// Callers (the InferenceService reconciler) pass hasDecoder /
// hasRouter from the merged runtime spec, so a decoder/router added
// by the runtime template participates in traffic management even
// when not declared on the ISVC itself.
func ComputeTargetHTTPRoutes(isvc *v1beta1.InferenceService, hasDecoder, hasRouter bool) []string {
	out := make([]string, 0, 4)
	out = append(out, isvc.Name)
	out = append(out, constants.EngineServiceName(isvc.Name))
	if hasDecoder {
		out = append(out, constants.DecoderServiceName(isvc.Name))
	}
	if hasRouter {
		out = append(out, constants.RouterServiceName(isvc.Name))
	}
	return out
}
