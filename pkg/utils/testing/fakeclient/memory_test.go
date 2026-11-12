package fakeclient

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// memorySlice is one EndpointSlice of a Service with one endpoint, for the
// reader's own tests.
func memorySlice(namespace, name, serviceName string) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels: map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		Endpoints: []discoveryv1.Endpoint{{
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: name + "-pod"},
		}},
	}
}

func listSlices(t *testing.T, reader client.Reader, opts ...client.ListOption) []discoveryv1.EndpointSlice {
	t.Helper()
	list := &discoveryv1.EndpointSliceList{}
	if err := reader.List(context.Background(), list, opts...); err != nil {
		t.Fatalf("List: %v", err)
	}
	return list.Items
}

func sliceNames(slices []discoveryv1.EndpointSlice) []string {
	var names []string
	for i := range slices {
		names = append(names, slices[i].Name)
	}
	return names
}

func TestMemoryReader_ListKeepsTheKindsObjectsOfTheNamespaceAndTheLabels(t *testing.T) {
	reader := ReadFromMemory(
		memorySlice("ns", "a-1", "a"), memorySlice("ns", "b-1", "b"),
		memorySlice("other", "a-2", "a"), memorySlice("ns", "a-3", "a"),
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", Labels: map[string]string{discoveryv1.LabelServiceName: "a"}}},
	)
	got := sliceNames(listSlices(t, reader, client.InNamespace("ns"), client.MatchingLabels{discoveryv1.LabelServiceName: "a"}))
	if want := []string{"a-1", "a-3"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("listed %v, want %v: the Service's slices of the namespace, in the order held", got, want)
	}
	if all := sliceNames(listSlices(t, reader)); len(all) != 4 {
		t.Fatalf("a list without options listed %v, want every slice", all)
	}
	if reader.Lists != 2 {
		t.Fatalf("Lists = %d, want 2", reader.Lists)
	}
}

func TestMemoryReader_ListCopiesEachMatchUnlessAskedForTheHeldObjects(t *testing.T) {
	held := memorySlice("ns", "a-1", "a")
	reader := ReadFromMemory(held)

	copied := listSlices(t, reader)
	if reader.Copies != 1 {
		t.Fatalf("Copies = %d after a plain list, want 1", reader.Copies)
	}
	if copied[0].Endpoints[0].TargetRef == held.Endpoints[0].TargetRef {
		t.Fatalf("a plain list handed out the held object's own endpoint target; want a deep copy")
	}

	own := listSlices(t, reader, client.UnsafeDisableDeepCopy)
	if reader.Copies != 1 {
		t.Fatalf("Copies = %d after an uncopied list, want 1 still", reader.Copies)
	}
	if own[0].Endpoints[0].TargetRef != held.Endpoints[0].TargetRef {
		t.Fatalf("an uncopied list handed out a copy; want the held object's own endpoint target")
	}
}

func TestMemoryReader_ListRefusesAFieldSelector(t *testing.T) {
	reader := ReadFromMemory(memorySlice("ns", "a-1", "a"))
	err := reader.List(context.Background(), &discoveryv1.EndpointSliceList{}, client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("metadata.name", "a-1")})
	if err == nil {
		t.Fatalf("a list by a field selector was answered; want a refusal, as the cache refuses a field without an index")
	}
}

func TestMemoryReader_GetCopiesUnlessAskedForTheHeldObject(t *testing.T) {
	held := memorySlice("ns", "a-1", "a")
	reader := ReadFromMemory(held)
	key := client.ObjectKeyFromObject(held)

	copied := &discoveryv1.EndpointSlice{}
	if err := reader.Get(context.Background(), key, copied); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if copied.Endpoints[0].TargetRef == held.Endpoints[0].TargetRef || reader.Copies != 1 {
		t.Fatalf("a plain Get handed out the held endpoint target (Copies = %d); want a deep copy", reader.Copies)
	}

	own := &discoveryv1.EndpointSlice{}
	if err := reader.Get(context.Background(), key, own, client.UnsafeDisableDeepCopy); err != nil {
		t.Fatalf("Get uncopied: %v", err)
	}
	if own.Endpoints[0].TargetRef != held.Endpoints[0].TargetRef || reader.Copies != 1 {
		t.Fatalf("an uncopied Get handed out a copy (Copies = %d); want the held object's own endpoint target", reader.Copies)
	}

	err := reader.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "missing"}, &discoveryv1.EndpointSlice{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Get of an object not held: %v, want NotFound", err)
	}
	if err := reader.Get(context.Background(), key, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Get of another kind under the same key: %v, want NotFound", err)
	}
}
