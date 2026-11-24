package main

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/tpuslice/gke"
)

const virtualLabel = "alfred-e2e/virtual-tpu"

// fixture is the provider against a fake API server holding the eight hosts
// of pod() and one pending 2x2x2 Slice s1. failures counts down injected
// write failures per target: "status", "slice" or a node name.
type fixture struct {
	t        *testing.T
	c        client.Client
	p        *provider
	failures map[string]int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	x := &fixture{t: t, failures: map[string]int{}}
	var objs []client.Object
	for _, h := range pod() {
		lbls := map[string]string{virtualLabel: "true", acceleratorLabel: "tpu7x"}
		for shape, id := range h.ids {
			lbls[idPrefix+shape+idSuffix] = id
			lbls[idPrefix+shape+stateSuffix] = healthyState
		}
		objs = append(objs, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: h.name, Labels: lbls}})
	}
	s, err := gke.Build(gke.Spec{Name: "s1", Type: "tpu7x", Topology: "2x2x2", Owner: map[string]string{"owner": "test"}})
	if err != nil {
		t.Fatal(err)
	}
	objs = append(objs, s)
	fail := func(target string) error {
		if x.failures[target] > 0 {
			x.failures[target]--
			return errors.New("injected failure")
		}
		return nil
	}
	x.c = fake.NewClientBuilder().WithObjects(objs...).WithStatusSubresource(gke.NewObject()).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				target := obj.GetName()
				if _, ok := obj.(*unstructured.Unstructured); ok {
					target = "slice"
				}
				if err := fail(target); err != nil {
					return err
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if err := fail(sub); err != nil {
					return err
				}
				return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		}).Build()
	x.p = &provider{c: x.c, selector: labels.SelectorFromSet(labels.Set{virtualLabel: "true"})}
	return x
}

func (x *fixture) reconcile(wantErr bool) {
	x.t.Helper()
	if err := x.p.reconcile(context.Background()); (err != nil) != wantErr {
		x.t.Fatalf("reconcile() error = %v, want error %t", err, wantErr)
	}
}

// slice returns s1's partitions and state.
func (x *fixture) slice() ([]string, string) {
	x.t.Helper()
	u := gke.NewObject()
	if err := x.c.Get(context.Background(), client.ObjectKey{Name: "s1"}, u); err != nil {
		x.t.Fatal(err)
	}
	s, err := gke.Parse(u)
	if err != nil {
		x.t.Fatal(err)
	}
	return s.PartitionIDs, s.State
}

// labeled returns the hosts carrying s1's label.
func (x *fixture) labeled() []string {
	x.t.Helper()
	nodes := &corev1.NodeList{}
	if err := x.c.List(context.Background(), nodes); err != nil {
		x.t.Fatal(err)
	}
	var out []string
	for _, n := range nodes.Items {
		if n.Labels[sliceLabel] == "s1" {
			out = append(out, n.Name)
		}
	}
	sort.Strings(out)
	return out
}

func (x *fixture) want(partition string, hosts ...string) {
	x.t.Helper()
	ids, state := x.slice()
	if !reflect.DeepEqual(ids, []string{partition}) || state != activeState || !reflect.DeepEqual(x.labeled(), hosts) {
		x.t.Fatalf("s1 = %v %q on %v, want [%s] %s on %v", ids, state, x.labeled(), partition, activeState, hosts)
	}
}

// TestReconcileActivatesAfterAFailedStatusUpdate: s1 was assigned d0, then
// its status update failed. The next pass activates it on d0.
func TestReconcileActivatesAfterAFailedStatusUpdate(t *testing.T) {
	x := newFixture(t)
	x.failures["status"] = 1
	x.reconcile(true)
	if ids, state := x.slice(); !reflect.DeepEqual(ids, []string{"d0"}) || state == activeState {
		t.Fatalf("after the failed update s1 = %v %q, want d0 and not active", ids, state)
	}
	x.reconcile(false)
	x.want("d0", "tpu-0", "tpu-1")
}

// TestReconcileResumesAnInterruptedPlacement: d0's hosts were labeled for s1,
// then recording the partition on s1 failed. The next pass records d0 rather
// than reserving a second partition.
func TestReconcileResumesAnInterruptedPlacement(t *testing.T) {
	x := newFixture(t)
	x.failures["slice"] = 1
	x.reconcile(true)
	x.reconcile(false)
	x.want("d0", "tpu-0", "tpu-1")
}

// TestReconcileReleasesAStaleReservation: tpu-0 was labeled for s1, labeling
// tpu-1 failed, and tpu-1 was then cordoned. The next pass releases tpu-0
// instead of placing s1 on a cordoned host; the pass after places s1 afresh.
func TestReconcileReleasesAStaleReservation(t *testing.T) {
	x := newFixture(t)
	x.failures["tpu-1"] = 1
	x.reconcile(true)
	if got := x.labeled(); !reflect.DeepEqual(got, []string{"tpu-0"}) {
		t.Fatalf("labeled = %v, want tpu-0", got)
	}
	node := &corev1.Node{}
	if err := x.c.Get(context.Background(), client.ObjectKey{Name: "tpu-1"}, node); err != nil {
		t.Fatal(err)
	}
	node.Spec.Unschedulable = true
	if err := x.c.Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	x.reconcile(false)
	if ids, _ := x.slice(); len(ids) != 0 || len(x.labeled()) != 0 {
		t.Fatalf("s1 = %v on %v, want the reservation released", ids, x.labeled())
	}
	x.reconcile(false)
	x.want("d1", "tpu-2", "tpu-3")
}
