package gke

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var (
	ownerA = map[string]string{"example.com/owner-uid": "a"}
	ownerB = map[string]string{"example.com/owner-uid": "b"}
)

func testSpec() Spec {
	return Spec{
		Name:        "ns-svc-engine-0",
		Type:        "tpu-a",
		Topology:    "2x2x1",
		Owner:       map[string]string{"example.com/owner-uid": "a"},
		Labels:      map[string]string{"example.com/owner-kind": "Replica"},
		Annotations: map[string]string{"example.com/managed-by": "scheduler"},
	}
}

// stored returns spec as the API server would hold it.
func stored(t *testing.T, spec Spec, uid types.UID, mutate ...func(*unstructured.Unstructured)) *unstructured.Unstructured {
	t.Helper()
	u, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	u.SetUID(uid)
	for _, m := range mutate {
		m(u)
	}
	return u
}

func withState(reason, message string) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		u.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Other", "reason": "IGNORED"},
				map[string]interface{}{"type": "Ready", "status": "True", "reason": reason, "message": message},
			},
		}
	}
}

func withPartitions(ids ...interface{}) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		u.Object["spec"].(map[string]interface{})["partitionIds"] = ids
	}
}

func withFinalizer(u *unstructured.Unstructured) { u.SetFinalizers([]string{"example.com/hold"}) }

func newFakeClient(objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().WithObjects(objs...).Build()
}

func TestBuild(t *testing.T) {
	u, err := Build(testSpec())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := u.GroupVersionKind(); got != (schema.GroupVersionKind{Group: Group, Version: Version, Kind: Kind}) {
		t.Fatalf("GVK = %v", got)
	}
	if u.GetName() != "ns-svc-engine-0" || u.GetNamespace() != "" {
		t.Fatalf("name = %q/%q, want cluster-scoped ns-svc-engine-0", u.GetNamespace(), u.GetName())
	}
	wantLabels := map[string]string{"example.com/owner-uid": "a", "example.com/owner-kind": "Replica"}
	if diff := cmp.Diff(wantLabels, u.GetLabels()); diff != "" {
		t.Fatalf("labels mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"example.com/managed-by": "scheduler"}, u.GetAnnotations()); diff != "" {
		t.Fatalf("annotations mismatch (-want +got):\n%s", diff)
	}
	if len(u.GetOwnerReferences()) != 0 {
		t.Fatalf("owner references = %v, want none on a cluster-scoped Slice", u.GetOwnerReferences())
	}
	raw, err := json.Marshal(u.Object["spec"])
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	if want := `{"partitionIds":[],"topology":"2x2x1","type":"tpu-a"}`; string(raw) != want {
		t.Fatalf("spec = %s, want %s", raw, want)
	}
}

func TestBuildCopiesInputs(t *testing.T) {
	spec := testSpec()
	u, err := Build(spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	spec.Owner["example.com/owner-uid"] = "mutated"
	spec.Labels["example.com/owner-kind"] = "mutated"
	spec.Annotations["example.com/managed-by"] = "mutated"
	if u.GetLabels()["example.com/owner-uid"] != "a" || u.GetLabels()["example.com/owner-kind"] != "Replica" {
		t.Fatalf("labels alias the spec: %v", u.GetLabels())
	}
	if u.GetAnnotations()["example.com/managed-by"] != "scheduler" {
		t.Fatalf("annotations alias the spec: %v", u.GetAnnotations())
	}
}

func TestBuildRejects(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Spec)
		wantErr string
	}{
		{name: "empty name", mutate: func(s *Spec) { s.Name = "" }, wantErr: "slice name"},
		{name: "uppercase name", mutate: func(s *Spec) { s.Name = "Ns-svc" }, wantErr: "slice name"},
		{name: "dotted name", mutate: func(s *Spec) { s.Name = "ns.svc" }, wantErr: "slice name"},
		{name: "name over 63", mutate: func(s *Spec) { s.Name = strings.Repeat("a", 64) }, wantErr: "slice name"},
		{name: "no type", mutate: func(s *Spec) { s.Type = "" }, wantErr: "type and topology"},
		{name: "no topology", mutate: func(s *Spec) { s.Topology = "" }, wantErr: "type and topology"},
		{name: "no owner", mutate: func(s *Spec) { s.Owner = nil }, wantErr: "owner labels"},
		{
			name:    "label contradicts owner",
			mutate:  func(s *Spec) { s.Labels = map[string]string{"example.com/owner-uid": "b"} },
			wantErr: "owner label",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := testSpec()
			tt.mutate(&spec)
			if _, err := Build(spec); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Build error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestBuildAllowsLabelRepeatingOwner(t *testing.T) {
	spec := testSpec()
	spec.Labels = map[string]string{"example.com/owner-uid": "a"}
	if _, err := Build(spec); err != nil {
		t.Fatalf("Build: %v", err)
	}
}

func TestParse(t *testing.T) {
	now := metav1.Now()
	u := stored(t, testSpec(), "uid-1", withState("ACTIVE", "provisioned"), withPartitions("p-1", "p-2"), func(u *unstructured.Unstructured) {
		u.SetDeletionTimestamp(&now)
	})
	got, err := Parse(u)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := Slice{
		Name:         "ns-svc-engine-0",
		UID:          "uid-1",
		Labels:       map[string]string{"example.com/owner-uid": "a", "example.com/owner-kind": "Replica"},
		Type:         "tpu-a",
		Topology:     "2x2x1",
		Terminating:  true,
		State:        "ACTIVE",
		Message:      "provisioned",
		PartitionIDs: []string{"p-1", "p-2"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Parse mismatch (-want +got):\n%s", diff)
	}
}

func TestParseWithoutStatus(t *testing.T) {
	got, err := Parse(stored(t, testSpec(), "uid-1"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.State != "" || got.Message != "" || got.Terminating || got.PartitionIDs != nil {
		t.Fatalf("Parse = %+v, want no state, no partitions and not terminating", got)
	}
}

func TestParseRejects(t *testing.T) {
	wrongKind := stored(t, testSpec(), "uid-1")
	wrongKind.SetKind("Other")
	if _, err := Parse(wrongKind); err == nil {
		t.Fatal("Parse accepted another kind")
	}
	badType := stored(t, testSpec(), "uid-1")
	badType.Object["spec"].(map[string]interface{})["type"] = int64(7)
	if _, err := Parse(badType); err == nil {
		t.Fatal("Parse accepted a non-string spec.type")
	}
	if _, err := Parse(stored(t, testSpec(), "uid-1", withPartitions(int64(7)))); err == nil {
		t.Fatal("Parse accepted a non-string partition ID")
	}
	badConditions := stored(t, testSpec(), "uid-1")
	badConditions.Object["status"] = map[string]interface{}{"conditions": "ACTIVE"}
	if _, err := Parse(badConditions); err == nil {
		t.Fatal("Parse accepted non-list conditions")
	}
}

func TestReady(t *testing.T) {
	readyStates := []string{"ACTIVE", "ACTIVE_DEGRADED"}
	tests := []struct {
		name  string
		slice Slice
		want  bool
	}{
		{name: "active", slice: Slice{State: "ACTIVE"}, want: true},
		{name: "degraded", slice: Slice{State: "ACTIVE_DEGRADED"}, want: true},
		{name: "provisioning", slice: Slice{State: "CREATING"}},
		{name: "no state", slice: Slice{}},
		{name: "terminating", slice: Slice{State: "ACTIVE", Terminating: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.slice.Ready(readyStates); got != tt.want {
				t.Fatalf("Ready = %v, want %v", got, tt.want)
			}
		})
	}
	if (Slice{}).Ready([]string{""}) {
		t.Fatal("a Slice with no state is ready when the empty state is listed")
	}
	if (Slice{State: "ACTIVE"}).Ready(nil) {
		t.Fatal("a Slice is ready with no ready states configured")
	}
}

func TestOwnedByAndMatches(t *testing.T) {
	s := Slice{Labels: map[string]string{"k1": "v1", "k2": "v2"}, Type: "tpu-a", Topology: "2x2x1"}
	if !s.OwnedBy(map[string]string{"k1": "v1"}) || !s.OwnedBy(map[string]string{"k1": "v1", "k2": "v2"}) {
		t.Fatal("OwnedBy rejects a matching owner")
	}
	if s.OwnedBy(map[string]string{"k1": "other"}) || s.OwnedBy(map[string]string{"k3": "v3"}) {
		t.Fatal("OwnedBy accepts a different owner")
	}
	if s.OwnedBy(nil) {
		t.Fatal("OwnedBy accepts the empty owner")
	}
	if !s.Matches(Spec{Type: "tpu-a", Topology: "2x2x1"}) {
		t.Fatal("Matches rejects the same shape")
	}
	if s.Matches(Spec{Type: "tpu-a", Topology: "2x2x2"}) || s.Matches(Spec{Type: "tpu-b", Topology: "2x2x1"}) {
		t.Fatal("Matches accepts a different shape")
	}
}

func TestCreate(t *testing.T) {
	c := newFakeClient()
	slices := NewClient(c, c)
	got, created, err := slices.Create(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("Create did not report creating the Slice")
	}
	if got.Name != "ns-svc-engine-0" || got.Type != "tpu-a" || got.Topology != "2x2x1" || !got.OwnedBy(ownerA) {
		t.Fatalf("Create = %+v", got)
	}
	u := NewObject()
	if err := c.Get(context.Background(), client.ObjectKey{Name: "ns-svc-engine-0"}, u); err != nil {
		t.Fatalf("stored Slice: %v", err)
	}
	ids, found, err := unstructured.NestedSlice(u.Object, "spec", "partitionIds")
	if err != nil || !found || len(ids) != 0 {
		t.Fatalf("stored partitionIds = %v (found %v, err %v), want present and empty", ids, found, err)
	}
}

func TestCreateReturnsOwnedExisting(t *testing.T) {
	existing := stored(t, testSpec(), "uid-1", withState("ACTIVE", ""))
	c := newFakeClient(existing)
	got, created, err := NewClient(c, c).Create(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created {
		t.Fatal("Create reported creating a Slice that already existed")
	}
	if got.UID != "uid-1" || got.State != "ACTIVE" {
		t.Fatalf("Create = %+v, want the existing Slice", got)
	}
}

func TestCreateRefusesForeignExisting(t *testing.T) {
	foreignSpec := testSpec()
	foreignSpec.Owner = ownerB
	foreignSpec.Topology = "2x2x2"
	c := newFakeClient(stored(t, foreignSpec, "uid-foreign"))
	_, _, err := NewClient(c, c).Create(context.Background(), testSpec())
	if !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("Create error = %v, want ErrOwnershipConflict", err)
	}
	s, found, err := NewClient(c, c).Get(context.Background(), "ns-svc-engine-0")
	if err != nil || !found || s.UID != "uid-foreign" || s.Topology != "2x2x2" || !s.OwnedBy(ownerB) {
		t.Fatalf("foreign Slice changed: %+v (found %v, err %v)", s, found, err)
	}
}

func TestCreateRejectsInvalidSpec(t *testing.T) {
	c := newFakeClient()
	spec := testSpec()
	spec.Owner = nil
	if _, _, err := NewClient(c, c).Create(context.Background(), spec); err == nil {
		t.Fatal("Create accepted a Slice without owner labels")
	}
}

func TestGet(t *testing.T) {
	c := newFakeClient(stored(t, testSpec(), "uid-1"))
	slices := NewClient(c, c)
	s, found, err := slices.Get(context.Background(), "ns-svc-engine-0")
	if err != nil || !found || s.UID != "uid-1" {
		t.Fatalf("Get = %+v, %v, %v", s, found, err)
	}
	if _, found, err := slices.Get(context.Background(), "missing"); err != nil || found {
		t.Fatalf("Get(missing) found=%v err=%v, want not found", found, err)
	}
}

func TestList(t *testing.T) {
	a1 := testSpec()
	a1.Name = "b-slice"
	a2 := testSpec()
	a2.Name = "a-slice"
	b := testSpec()
	b.Name = "c-slice"
	b.Owner = ownerB
	c := newFakeClient(stored(t, a1, "uid-1"), stored(t, a2, "uid-2"), stored(t, b, "uid-3"))
	got, err := NewClient(c, c).List(context.Background(), labels.SelectorFromSet(ownerA))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var names []string
	for _, s := range got {
		names = append(names, s.Name)
	}
	if diff := cmp.Diff([]string{"a-slice", "b-slice"}, names); diff != "" {
		t.Fatalf("List names mismatch (-want +got):\n%s", diff)
	}
}

func TestListRequiresSelector(t *testing.T) {
	c := newFakeClient()
	for _, sel := range []labels.Selector{nil, labels.Everything()} {
		if _, err := NewClient(c, c).List(context.Background(), sel); err == nil {
			t.Fatalf("List(%v) accepted an empty selector", sel)
		}
	}
}

// uidPreconditionClient enforces Preconditions.UID on Delete, which the fake
// client ignores.
func uidPreconditionClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			var do client.DeleteOptions
			do.ApplyOptions(opts)
			if do.Preconditions == nil || do.Preconditions.UID == nil {
				t.Errorf("delete of %s without a UID precondition", obj.GetName())
				return c.Delete(ctx, obj, opts...)
			}
			current := NewObject()
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
				return err
			}
			if current.GetUID() != *do.Preconditions.UID {
				return apierrors.NewConflict(schema.GroupResource{Group: Group, Resource: "slices"}, obj.GetName(),
					errors.New("uid precondition failed"))
			}
			return c.Delete(ctx, obj, opts...)
		},
	}).Build()
}

func TestRelease(t *testing.T) {
	ctx := context.Background()
	c := uidPreconditionClient(t, stored(t, testSpec(), "uid-1"))
	slices := NewClient(c, c)
	observed, _, err := slices.Get(ctx, "ns-svc-engine-0")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	complete, err := slices.Release(ctx, observed, ownerA)
	if err != nil || complete {
		t.Fatalf("Release = %v, %v; want an accepted, incomplete delete", complete, err)
	}
	if _, found, _ := slices.Get(ctx, "ns-svc-engine-0"); found {
		t.Fatal("Slice still exists after Release")
	}
	complete, err = slices.Release(ctx, observed, ownerA)
	if err != nil || !complete {
		t.Fatalf("second Release = %v, %v; want complete", complete, err)
	}
}

func TestReleaseWaitsOnFinalizer(t *testing.T) {
	ctx := context.Background()
	c := uidPreconditionClient(t, stored(t, testSpec(), "uid-1", withFinalizer))
	slices := NewClient(c, c)
	observed, _, _ := slices.Get(ctx, "ns-svc-engine-0")
	if complete, err := slices.Release(ctx, observed, ownerA); err != nil || complete {
		t.Fatalf("Release = %v, %v; want an accepted, incomplete delete", complete, err)
	}
	terminating, found, err := slices.Get(ctx, "ns-svc-engine-0")
	if err != nil || !found || !terminating.Terminating {
		t.Fatalf("Get after Release = %+v, %v, %v; want a terminating Slice", terminating, found, err)
	}
	if complete, err := slices.Release(ctx, terminating, ownerA); err != nil || complete {
		t.Fatalf("Release of a terminating Slice = %v, %v; want incomplete, no error", complete, err)
	}
}

func TestReleaseSparesReplacement(t *testing.T) {
	ctx := context.Background()
	c := uidPreconditionClient(t, stored(t, testSpec(), "uid-2"))
	slices := NewClient(c, c)
	old := Slice{Name: "ns-svc-engine-0", UID: "uid-1", Labels: ownerA}
	if complete, err := slices.Release(ctx, old, ownerA); err == nil || complete {
		t.Fatalf("Release of a replaced Slice = %v, %v; want a conflict", complete, err)
	}
	if s, found, _ := slices.Get(ctx, "ns-svc-engine-0"); !found || s.UID != "uid-2" {
		t.Fatalf("replacement Slice deleted: %+v, found %v", s, found)
	}
}

func TestReleaseRefuses(t *testing.T) {
	ctx := context.Background()
	c := uidPreconditionClient(t, stored(t, testSpec(), "uid-1"))
	slices := NewClient(c, c)
	observed, _, _ := slices.Get(ctx, "ns-svc-engine-0")
	if _, err := slices.Release(ctx, observed, ownerB); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("Release by another owner error = %v, want ErrOwnershipConflict", err)
	}
	unobserved := observed
	unobserved.UID = ""
	if _, err := slices.Release(ctx, unobserved, ownerA); err == nil {
		t.Fatal("Release accepted a Slice without a UID")
	}
	if _, found, _ := slices.Get(ctx, "ns-svc-engine-0"); !found {
		t.Fatal("refused Release deleted the Slice")
	}
}

func TestCRDNotInstalled(t *testing.T) {
	noMatch := &apimeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: Group, Kind: Kind}, SearchedVersions: []string{Version}}
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return noMatch
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return noMatch
		},
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			return noMatch
		},
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return noMatch
		},
	}).Build()
	ctx := context.Background()
	slices := NewClient(c, c)
	if _, found, err := slices.Get(ctx, "ns-svc-engine-0"); err != nil || found {
		t.Fatalf("Get = found %v, err %v; want not found", found, err)
	}
	if got, err := slices.List(ctx, labels.SelectorFromSet(ownerA)); err != nil || len(got) != 0 {
		t.Fatalf("List = %v, %v; want none", got, err)
	}
	if _, _, err := slices.Create(ctx, testSpec()); err == nil {
		t.Fatal("Create succeeded without the Slice CRD")
	}
	complete, err := slices.Release(ctx, Slice{Name: "ns-svc-engine-0", UID: "uid-1", Labels: ownerA}, ownerA)
	if err != nil || !complete {
		t.Fatalf("Release = %v, %v; want complete", complete, err)
	}
}

func TestGetPropagatesErrors(t *testing.T) {
	boom := errors.New("boom")
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return boom
		},
	}).Build()
	ctx := context.Background()
	slices := NewClient(c, c)
	if _, _, err := slices.Get(ctx, "x"); !errors.Is(err, boom) {
		t.Fatalf("Get error = %v, want boom", err)
	}
	if _, err := slices.List(ctx, labels.SelectorFromSet(ownerA)); !errors.Is(err, boom) {
		t.Fatalf("List error = %v, want boom", err)
	}
}
