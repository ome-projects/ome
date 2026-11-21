package fieldequal

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func ownerReferenceWithEveryFieldSet() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "fleet", UID: "fleet-uid",
		Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
	}
}

// The compare answers as the semantic compare answers: a nil list and an
// empty one are equal, a list and its copy are equal, and a list that
// differs in one leaf of one reference, in its length or in its order is
// different. Every field of an owner reference is a row, so a field added
// to the type fails the count below until it is read.
func TestOwnerReferences_AgreesWithTheSemanticCompare(t *testing.T) {
	base := []metav1.OwnerReference{ownerReferenceWithEveryFieldSet()}
	changes := map[string]func(refs []metav1.OwnerReference) []metav1.OwnerReference{
		"apiVersion":             func(r []metav1.OwnerReference) []metav1.OwnerReference { r[0].APIVersion += "x"; return r },
		"kind":                   func(r []metav1.OwnerReference) []metav1.OwnerReference { r[0].Kind += "x"; return r },
		"name":                   func(r []metav1.OwnerReference) []metav1.OwnerReference { r[0].Name += "x"; return r },
		"uid":                    func(r []metav1.OwnerReference) []metav1.OwnerReference { r[0].UID += "x"; return r },
		"controller=nil":         func(r []metav1.OwnerReference) []metav1.OwnerReference { r[0].Controller = nil; return r },
		"controller=false":       func(r []metav1.OwnerReference) []metav1.OwnerReference { r[0].Controller = ptr.To(false); return r },
		"blockOwnerDeletion=nil": func(r []metav1.OwnerReference) []metav1.OwnerReference { r[0].BlockOwnerDeletion = nil; return r },
		"blockOwnerDeletion=false": func(r []metav1.OwnerReference) []metav1.OwnerReference {
			r[0].BlockOwnerDeletion = ptr.To(false)
			return r
		},
		"one reference more": func(r []metav1.OwnerReference) []metav1.OwnerReference { return append(r, metav1.OwnerReference{}) },
		"emptied":            func([]metav1.OwnerReference) []metav1.OwnerReference { return nil },
	}
	if got := reflect.TypeOf(metav1.OwnerReference{}).NumField(); got != 6 {
		t.Fatalf("an owner reference has %d fields, this test knows 6; read the new field in ownerReference and add its rows", got)
	}
	for name, change := range changes {
		changed := change(cloneOwnerReferences(base))
		want := equality.Semantic.DeepEqual(base, changed)
		if want {
			t.Errorf("%s: the semantic compare reads the changed list as equal; the change is not one", name)
		}
		if got := OwnerReferences(base, changed); got != want {
			t.Errorf("%s: OwnerReferences = %v, the semantic compare says %v", name, got, want)
		}
		if !OwnerReferences(changed, cloneOwnerReferences(changed)) {
			t.Errorf("%s: the changed list and its copy read as different", name)
		}
	}
	if !equality.Semantic.DeepEqual([]metav1.OwnerReference{}, []metav1.OwnerReference(nil)) || !OwnerReferences([]metav1.OwnerReference{}, nil) {
		t.Errorf("an empty list and a nil one read as different")
	}
	second := metav1.OwnerReference{Kind: "InferenceReplica", Name: "fleet-engine", UID: "fleet-engine-uid"}
	inOrder, reversed := []metav1.OwnerReference{base[0], second}, []metav1.OwnerReference{second, base[0]}
	if equality.Semantic.DeepEqual(inOrder, reversed) || OwnerReferences(inOrder, reversed) {
		t.Errorf("two references in the other order read as equal")
	}
}

func cloneOwnerReferences(refs []metav1.OwnerReference) []metav1.OwnerReference {
	if refs == nil {
		return nil
	}
	out := make([]metav1.OwnerReference, len(refs))
	for i := range refs {
		refs[i].DeepCopyInto(&out[i])
	}
	return out
}

// Two absent values are equal, one absent is a difference, two present
// compare by value, as the semantic compare reads pointers.
func TestPointers_AgreesWithTheSemanticCompare(t *testing.T) {
	for name, pair := range map[string][2]*int32{
		"both absent":            {nil, nil},
		"one absent":             {nil, ptr.To[int32](3)},
		"the same value":         {ptr.To[int32](3), ptr.To[int32](3)},
		"two different values":   {ptr.To[int32](3), ptr.To[int32](4)},
		"a zero and an absent":   {ptr.To[int32](0), nil},
		"two zeros held apart":   {ptr.To[int32](0), ptr.To[int32](0)},
		"the same pointer twice": func() [2]*int32 { p := ptr.To[int32](5); return [2]*int32{p, p} }(),
	} {
		want := equality.Semantic.DeepEqual(pair[0], pair[1])
		if got := Pointers(pair[0], pair[1]); got != want {
			t.Errorf("%s: Pointers = %v, the semantic compare says %v", name, got, want)
		}
	}
	if !Pointers(ptr.To("h2c"), ptr.To("h2c")) || Pointers(ptr.To("h2c"), ptr.To("")) || Pointers[string](nil, ptr.To("")) {
		t.Errorf("Pointers over strings does not compare by value with an absent side a difference")
	}
}
