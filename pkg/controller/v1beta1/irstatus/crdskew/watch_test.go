package crdskew

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func crdMeta(name string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestCRDNameKeepsOnlyTheManagersGroup(t *testing.T) {
	ours := "things." + v1beta1.SchemeGroupVersion.Group
	if got := crdName(crdMeta(ours)); got != ours {
		t.Fatalf("crdName = %q, want %q", got, ours)
	}
	if got := crdName(crdMeta("things.other.example")); got != "" {
		t.Fatalf("a CRD of another group is followed: %q", got)
	}
	if got := crdName(toolscache.DeletedFinalStateUnknown{Key: ours, Obj: crdMeta(ours)}); got != ours {
		t.Fatalf("a deleted CRD's last state is not read: %q", got)
	}
	if got := crdName("not an object"); got != "" {
		t.Fatalf("a value that is no object is followed: %q", got)
	}
}

func TestNameSetCoalescesNamesBehindOneSignal(t *testing.T) {
	set := newNameSet()
	set.add("b")
	set.add("a")
	set.add("b")
	if len(set.signal) != 1 {
		t.Fatalf("signals = %d, want one for any number of names", len(set.signal))
	}
	<-set.signal
	if got := set.drain(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("drain = %v, want [a b]", got)
	}
	if got := set.drain(); len(got) != 0 {
		t.Fatalf("a second drain returns %v", got)
	}
}
