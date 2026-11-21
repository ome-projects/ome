// Package fieldequal compares the fields a controller owns on an object
// under the rule of the apiserver's semantic compare, without reflection: a
// nil list equals an empty one, and an optional value set on one side alone
// is a difference. A writer that decides "nothing to write" from the fields
// it owns reads them through these on every pass.
package fieldequal

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// OwnerReferences reads two owner reference lists in order, every field of
// each reference; a nil list and an empty one are the same.
func OwnerReferences(a, b []metav1.OwnerReference) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !ownerReference(&a[i], &b[i]) {
			return false
		}
	}
	return true
}

// ownerReference reads every field of an owner reference. A field added to
// the type is read here too: the contract tests of the compares built on
// this change every leaf of an owner reference one at a time and fail on a
// field this function skips.
func ownerReference(a, b *metav1.OwnerReference) bool {
	return a.APIVersion == b.APIVersion &&
		a.Kind == b.Kind &&
		a.Name == b.Name &&
		a.UID == b.UID &&
		Pointers(a.Controller, b.Controller) &&
		Pointers(a.BlockOwnerDeletion, b.BlockOwnerDeletion)
}

// Pointers reads two optional values; both absent is equal, one absent is
// a difference.
func Pointers[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
