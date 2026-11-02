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
		if err := r.cleanupPriorPolicy(ctx, isvc); err != nil {
			return nil, err
		}
		return nil, nil
	}

	emitted, passthroughs, translateErr := r.translator.Translate(isvc, targetHTTPRoutes, intent)

	// Apply the emitted resource only when the translator succeeded
	// and produced something to apply. An apply failure folds into the
	// translateErr surface for the status writer.
	var conflictMessage string
	if translateErr == nil && emitted != nil {
		if applyErr := r.applyPolicy(ctx, isvc, emitted); applyErr != nil {
			translateErr = applyErr
			if errors.Is(applyErr, ErrConflictingPolicy) {
				conflictMessage = applyErr.Error()
			}
		}
	}

	// Observe gateway acceptance from the post-apply state on the
	// server. Only meaningful when we successfully applied something
	// — on first reconcile we usually get Pending (gateway hasn't
	// run yet) and the next .Owns()-driven re-enqueue surfaces the
	// real signal. Skip the observe step when we hit a conflict
	// (we didn't write anything; existing object's status is not
	// ours to interpret).
	acceptance := AcceptanceObservation{State: AcceptancePending}
	if translateErr == nil && emitted != nil {
		acceptance = r.observeAcceptance(ctx, emitted)
	}

	out := status.Build(status.BuildArgs{
		TranslatorName:           r.translator.Name(),
		HasIntent:                true,
		Algorithm:                algorithmOf(intent),
		EmittedPolicy:            emitted,
		TargetedRoutes:           targetHTTPRoutes,
		Passthroughs:             passthroughs,
		TranslateErr:             translateErr,
		ConflictDetected:         conflictMessage != "",
		ConflictMessage:          conflictMessage,
		ObservedGeneration:       isvc.Generation,
		Now:                      r.now(),
		GatewayAcceptance:        acceptance.State,
		GatewayAcceptanceReason:  acceptance.Reason,
		GatewayAcceptanceMessage: acceptance.Message,
		UnsupportedAnnotations:   ComputeUnsupportedAnnotations(isvc.Annotations, r.translator),
		UnsupportedFields:        ComputeUnsupportedTrafficFields(intent.Traffic, r.translator),
	})

	return out, translateErr
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
	// Noop translator never emits anything, so there's nothing to
	// clean up. Defensive — in practice the gate above already short-
	// circuits because the noop branch never sets BackendPolicyResource.
	watch := r.translator.Watches()
	if watch == nil {
		return nil
	}
	obj, ok := watch.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("translator.Watches() did not produce a client.Object")
	}
	// Translator emits with isvc.Name; the status mirrors that, so
	// either works. Going through status keeps us robust to future
	// translators that pick a different name convention.
	obj.SetName(isvc.Status.Traffic.BackendPolicyResource.Name)
	obj.SetNamespace(isvc.Namespace)
	if err := r.client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete prior backend policy %s/%s: %w", isvc.Namespace, obj.GetName(), err)
	}
	return nil
}

// observeAcceptance fetches the freshly-applied policy resource and
// asks the translator to interpret its status. Errors fetching the
// object degrade silently to Pending — the next reconcile will retry.
func (r *Reconciler) observeAcceptance(ctx context.Context, desired client.Object) AcceptanceObservation {
	fetched, ok := desired.DeepCopyObject().(client.Object)
	if !ok {
		return AcceptanceObservation{State: AcceptancePending}
	}
	// Strip the desired spec; Get fills it in (including status).
	fetched.SetName(desired.GetName())
	fetched.SetNamespace(desired.GetNamespace())
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(desired), fetched); err != nil {
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

// applyPolicy sets controller reference and create-or-updates the
// emitted policy resource. The translator guarantees deterministic
// output, so the update path can be a coarse Update; finer
// reconciliation lives in the translator (e.g. semantic equality
// helpers) when needed.
func (r *Reconciler) applyPolicy(ctx context.Context, isvc *v1beta1.InferenceService, desired client.Object) error {
	if err := controllerutil.SetControllerReference(isvc, desired, r.scheme); err != nil {
		return fmt.Errorf("set controller reference on backend policy: %w", err)
	}

	// We need an empty instance of the same type to fetch into.
	existingObj, ok := desired.DeepCopyObject().(client.Object)
	if !ok {
		return errors.New("backend policy resource does not implement client.Object")
	}
	// Strip the desired fields out of the fetch target — Get fills it
	// in from the API server. Leaving the desired spec in place would
	// shadow the live state on a not-found path.
	existingObj.SetName(desired.GetName())
	existingObj.SetNamespace(desired.GetNamespace())

	key := client.ObjectKeyFromObject(desired)
	err := r.client.Get(ctx, key, existingObj)
	switch {
	case apierrors.IsNotFound(err):
		if createErr := r.client.Create(ctx, desired); createErr != nil {
			return fmt.Errorf("create backend policy %s: %w", key, createErr)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get backend policy %s: %w", key, err)
	}

	// ConflictingPolicy guard: if the existing object isn't owned by
	// this InferenceService, refuse to overwrite. This defers to hand-
	// authored policies of the same name (alpha-phase behavior); later
	// phases will warn (beta) or reject without an ack annotation (GA).
	if !ownedBy(existingObj, isvc) {
		return fmt.Errorf(
			"%w: %s already exists with %s",
			ErrConflictingPolicy, key, describeController(existingObj),
		)
	}

	// Update path. Preserve ResourceVersion so the Update doesn't lose
	// the optimistic-concurrency token. The translator's deterministic-
	// output contract means identical desired states across reconciles
	// — Update is idempotent for our purposes.
	desired.SetResourceVersion(existingObj.GetResourceVersion())
	if updateErr := r.client.Update(ctx, desired); updateErr != nil {
		return fmt.Errorf("update backend policy %s: %w", key, updateErr)
	}
	return nil
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
