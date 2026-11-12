package pdb

import (
	"context"
	"fmt"
	"maps"

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omevalidation "sigs.k8s.io/ome/pkg/validation"
)

// Budget is the availability constraint managed on a PodDisruptionBudget.
type Budget struct {
	MinAvailable   *intstr.IntOrString
	MaxUnavailable *intstr.IntOrString
}

// Request describes one component PodDisruptionBudget reconciliation.
type Request struct {
	Owner      *v1beta1.InferenceService
	ObjectMeta metav1.ObjectMeta
	Selector   map[string]string
	Budget     *Budget
	// CutoverFromSelector identifies the canonical selector of the source mode.
	// Only that selector is held while the target workload becomes ready.
	CutoverFromSelector map[string]string
	// SelectorCutoverReady permits replacing or removing an owned budget
	// whose live selector matches CutoverFromSelector.
	SelectorCutoverReady bool
}

// PDBReconciler reconciles component PodDisruptionBudgets.
type PDBReconciler struct {
	client client.Client
	reader client.Reader
	scheme *runtime.Scheme
}

// NewPDBReconcilerWithReader builds the reconciler on a writer and a live
// reader; a nil reader reads through the writer.
func NewPDBReconcilerWithReader(
	writer client.Client,
	reader client.Reader,
	scheme *runtime.Scheme,
) *PDBReconciler {
	if reader == nil {
		reader = writer
	}
	return &PDBReconciler{client: writer, reader: reader, scheme: scheme}
}

// Preflight verifies that a desired PodDisruptionBudget can be managed
// without changing cluster state.
func (r *PDBReconciler) Preflight(ctx context.Context, request Request) error {
	if err := validateRequest(request); err != nil {
		return err
	}
	if request.Budget == nil {
		return nil
	}

	existing := &policyv1.PodDisruptionBudget{}
	if err := r.reader.Get(ctx, requestKey(request), existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return validateDesiredExisting(existing, request.Owner)
}

// Reconcile ensures the requested PodDisruptionBudget state: a budget that
// does not exist is created when one is requested, an existing one is
// removed when none is, and otherwise it is brought to the requested shape.
func (r *PDBReconciler) Reconcile(ctx context.Context, request Request) (*policyv1.PodDisruptionBudget, error) {
	if err := validateRequest(request); err != nil {
		return nil, err
	}
	existing := &policyv1.PodDisruptionBudget{}
	if err := r.reader.Get(ctx, requestKey(request), existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		return r.createRequested(ctx, request)
	}
	if request.Budget == nil {
		return r.removeOwned(ctx, request, existing)
	}
	return r.updateExisting(ctx, request, existing)
}

// createRequested creates the budget the request asks for; a request for no
// budget creates nothing.
func (r *PDBReconciler) createRequested(ctx context.Context, request Request) (*policyv1.PodDisruptionBudget, error) {
	if request.Budget == nil {
		return nil, nil
	}
	desired, err := r.pdbForCreate(request)
	if err != nil {
		return nil, err
	}
	if err := r.client.Create(ctx, desired); err != nil {
		return nil, err
	}
	return desired, nil
}

// removeOwned deletes an existing budget when the request asks for none. A
// budget another controller owns, or one the source mode's selector still
// holds while the target workload becomes ready, is left as it is.
func (r *PDBReconciler) removeOwned(ctx context.Context, request Request, existing *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error) {
	if !controlledBy(existing, request.Owner) || selectorCutoverPending(request, existing) {
		return existing, nil
	}
	uid := existing.UID
	resourceVersion := existing.ResourceVersion
	err := r.client.Delete(ctx, existing, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	return nil, nil
}

// updateExisting brings an owned budget to the requested shape. A budget
// already in that shape, or one the source mode's selector still holds, is
// returned as it is.
func (r *PDBReconciler) updateExisting(ctx context.Context, request Request, existing *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error) {
	if err := validateDesiredExisting(existing, request.Owner); err != nil {
		return nil, err
	}
	if selectorCutoverPending(request, existing) || semanticPDBEquals(request, existing) {
		return existing, nil
	}
	updated := pdbForUpdate(request, existing)
	if err := r.client.Update(ctx, updated); err != nil {
		return nil, err
	}
	return updated, nil
}

func selectorCutoverPending(request Request, existing *policyv1.PodDisruptionBudget) bool {
	if request.SelectorCutoverReady || len(request.Selector) == 0 || len(request.CutoverFromSelector) == 0 {
		return false
	}
	desired := &metav1.LabelSelector{MatchLabels: request.Selector}
	source := &metav1.LabelSelector{MatchLabels: request.CutoverFromSelector}
	return !equality.Semantic.DeepEqual(desired, existing.Spec.Selector) &&
		equality.Semantic.DeepEqual(source, existing.Spec.Selector)
}

func requestKey(request Request) types.NamespacedName {
	return types.NamespacedName{
		Namespace: request.ObjectMeta.Namespace,
		Name:      request.ObjectMeta.Name,
	}
}

func validateDesiredExisting(existing *policyv1.PodDisruptionBudget, owner *v1beta1.InferenceService) error {
	if !controlledBy(existing, owner) {
		return ownershipConflict(existing, owner)
	}
	if existing.DeletionTimestamp != nil {
		return fmt.Errorf("PodDisruptionBudget %s/%s is terminating", existing.Namespace, existing.Name)
	}
	return nil
}

func validateRequest(request Request) error {
	if request.Owner == nil {
		return fmt.Errorf("PodDisruptionBudget owner is required")
	}
	if request.Owner.UID == "" {
		return fmt.Errorf("PodDisruptionBudget owner UID is required")
	}
	if request.ObjectMeta.Name == "" {
		return fmt.Errorf("PodDisruptionBudget name is required")
	}
	if request.ObjectMeta.Namespace == "" {
		return fmt.Errorf("PodDisruptionBudget namespace is required")
	}
	if request.Owner.Namespace != request.ObjectMeta.Namespace {
		return fmt.Errorf("PodDisruptionBudget namespace %q must match owner namespace %q", request.ObjectMeta.Namespace, request.Owner.Namespace)
	}
	if request.Budget == nil {
		return nil
	}
	// An empty policy/v1 selector selects every pod in the namespace.
	if len(request.Selector) == 0 {
		return fmt.Errorf("PodDisruptionBudget selector must not be empty")
	}
	if (request.Budget.MinAvailable == nil) == (request.Budget.MaxUnavailable == nil) {
		return fmt.Errorf("exactly one of minAvailable or maxUnavailable must be set")
	}
	return omevalidation.ValidatePodDisruptionBudget(
		"PodDisruptionBudget budget",
		request.Budget.MinAvailable,
		request.Budget.MaxUnavailable,
	)
}

func (r *PDBReconciler) pdbForCreate(request Request) (*policyv1.PodDisruptionBudget, error) {
	metadata := metav1.ObjectMeta{
		Name:            request.ObjectMeta.Name,
		Namespace:       request.ObjectMeta.Namespace,
		Labels:          maps.Clone(request.ObjectMeta.Labels),
		Annotations:     maps.Clone(request.ObjectMeta.Annotations),
		OwnerReferences: nonControllerOwnerReferences(request.ObjectMeta.OwnerReferences),
	}
	desired := &policyv1.PodDisruptionBudget{
		ObjectMeta: metadata,
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable:   copyIntOrString(request.Budget.MinAvailable),
			MaxUnavailable: copyIntOrString(request.Budget.MaxUnavailable),
			Selector: &metav1.LabelSelector{
				MatchLabels: maps.Clone(request.Selector),
			},
		},
	}
	if err := controllerutil.SetControllerReference(request.Owner, desired, r.scheme); err != nil {
		return nil, err
	}
	return desired, nil
}

func pdbForUpdate(request Request, existing *policyv1.PodDisruptionBudget) *policyv1.PodDisruptionBudget {
	updated := existing.DeepCopy()
	updated.Spec.MinAvailable = copyIntOrString(request.Budget.MinAvailable)
	updated.Spec.MaxUnavailable = copyIntOrString(request.Budget.MaxUnavailable)
	updated.Spec.Selector = &metav1.LabelSelector{MatchLabels: maps.Clone(request.Selector)}
	updated.OwnerReferences = normalizedOwnerReferences(existing.OwnerReferences, request.Owner)
	return updated
}

func semanticPDBEquals(request Request, existing *policyv1.PodDisruptionBudget) bool {
	controller := metav1.GetControllerOf(existing)
	return equality.Semantic.DeepEqual(request.Budget.MinAvailable, existing.Spec.MinAvailable) &&
		equality.Semantic.DeepEqual(request.Budget.MaxUnavailable, existing.Spec.MaxUnavailable) &&
		equality.Semantic.DeepEqual(
			&metav1.LabelSelector{MatchLabels: request.Selector},
			existing.Spec.Selector,
		) &&
		controller != nil &&
		equality.Semantic.DeepEqual(canonicalControllerReference(request.Owner), *controller)
}

func controlledBy(pdb *policyv1.PodDisruptionBudget, owner *v1beta1.InferenceService) bool {
	controller := metav1.GetControllerOf(pdb)
	return controller != nil && controller.UID == owner.UID
}

func ownershipConflict(pdb *policyv1.PodDisruptionBudget, owner *v1beta1.InferenceService) error {
	controller := metav1.GetControllerOf(pdb)
	if controller == nil {
		return apierrors.NewConflict(
			policyv1.Resource("poddisruptionbudgets"),
			pdb.Name,
			fmt.Errorf("object has no controller owner; expected InferenceService %s (UID %q)", owner.Name, owner.UID),
		)
	}
	return apierrors.NewConflict(
		policyv1.Resource("poddisruptionbudgets"),
		pdb.Name,
		fmt.Errorf("object is controlled by %s %s (UID %q), expected InferenceService %s (UID %q)", controller.Kind, controller.Name, controller.UID, owner.Name, owner.UID),
	)
}

func nonControllerOwnerReferences(refs []metav1.OwnerReference) []metav1.OwnerReference {
	kept := make([]metav1.OwnerReference, 0, len(refs))
	for _, ref := range refs {
		if ref.Controller == nil || !*ref.Controller {
			kept = append(kept, ref)
		}
	}
	return kept
}

func normalizedOwnerReferences(refs []metav1.OwnerReference, owner *v1beta1.InferenceService) []metav1.OwnerReference {
	normalized := make([]metav1.OwnerReference, 0, len(refs)+1)
	controllerAdded := false
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			if !controllerAdded {
				normalized = append(normalized, canonicalControllerReference(owner))
				controllerAdded = true
			}
			continue
		}
		normalized = append(normalized, ref)
	}
	if !controllerAdded {
		normalized = append(normalized, canonicalControllerReference(owner))
	}
	return normalized
}

func canonicalControllerReference(owner *v1beta1.InferenceService) metav1.OwnerReference {
	return *metav1.NewControllerRef(owner, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
}

func copyIntOrString(value *intstr.IntOrString) *intstr.IntOrString {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
