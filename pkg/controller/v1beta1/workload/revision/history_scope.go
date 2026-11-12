package revision

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The owners of a revision history. A projected replica's history belongs
// to the service that projects it as much as to the replica: every
// ControllerRevision names the replica as its controller and the service as
// a plain owner, so a replica deleted and projected again finds the
// revisions its predecessor minted, and the history leaves with the
// service. A standalone replica's history is its own.

// historyOwners are the owner references a new revision carries: the
// workload owner as its controller and, when the history has a scope
// other than the owner, the scope as a plain owner.
func historyOwners(owner client.Object, ownerGVK schema.GroupVersionKind, scope *metav1.OwnerReference) []metav1.OwnerReference {
	refs := []metav1.OwnerReference{*metav1.NewControllerRef(owner, ownerGVK)}
	if scope != nil && scope.UID != "" && scope.UID != owner.GetUID() {
		refs = append(refs, plainOwner(*scope))
	}
	return refs
}

// plainOwner is ref as a non-controller owner reference.
func plainOwner(ref metav1.OwnerReference) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: ref.APIVersion, Kind: ref.Kind, Name: ref.Name, UID: ref.UID}
}

// ScopeOwner is the object a replica's revision history belongs to beyond
// the replica: the controller of obj when it is of the given kind (the
// InferenceService that projects a replica), as a plain owner reference. nil
// when no such object controls obj, as for a standalone replica, whose
// history is its own.
func ScopeOwner(obj metav1.Object, kind schema.GroupKind) *metav1.OwnerReference {
	ref := metav1.GetControllerOf(obj)
	if ref == nil || ref.UID == "" || schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() != kind {
		return nil
	}
	owner := plainOwner(*ref)
	return &owner
}

// RetainedBy reports whether a ControllerRevision is part of the owner's
// history: the owner controls it, or the scope owns it. A revision a
// previous replica of the same service minted is the service's, so a
// replica projected again reads it as its own; one another service or an
// unknown object controls is not retained.
func RetainedBy(cr *appsv1.ControllerRevision, ownerUID types.UID, scope *metav1.OwnerReference) bool {
	if cr == nil {
		return false
	}
	if controllerOwnedBy(cr.OwnerReferences, ownerUID) {
		return true
	}
	return scope != nil && scope.UID != "" && ownedBy(cr.OwnerReferences, scope.UID)
}

// EnsureScopeOwner adds the scope as a plain owner of a revision that lacks
// it, so a revision minted before the history had a scope stays with the
// service as the new ones do. No scope, or one already present, writes
// nothing.
func EnsureScopeOwner(ctx context.Context, c client.Client, cr *appsv1.ControllerRevision, scope *metav1.OwnerReference) error {
	if c == nil || cr == nil || scope == nil || scope.UID == "" || ownedBy(cr.OwnerReferences, scope.UID) {
		return nil
	}
	patch := client.MergeFrom(cr.DeepCopy())
	cr.OwnerReferences = append(cr.OwnerReferences, plainOwner(*scope))
	if err := c.Patch(ctx, cr, patch); err != nil {
		return fmt.Errorf("add the scope owner to ControllerRevision %s: %w", cr.Name, err)
	}
	return nil
}

// ownedBy reports whether refs carries an owner reference, controller or
// not, pointing at uid.
func ownedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

// controllerOwnedBy reports whether refs contains a Controller=true
// OwnerReference pointing at uid. Only a true controller ref asserts
// the parent claim we care about.
func controllerOwnedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller && ref.UID == uid {
			return true
		}
	}
	return false
}
