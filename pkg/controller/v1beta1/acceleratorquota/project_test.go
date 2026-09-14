package acceleratorquota

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

const (
	hubOrigin   = "hub-1"
	projManager = "ome-quota-projector"
)

// fakeFleet hands out a fake client per member.
type fakeFleet struct {
	members map[string]workloadcluster.SelectivelyCachingClient
	// missing names a cluster Connected() reports but ClientFor does not have,
	// which is what a drop between the two calls looks like.
	missing string
}

func (f *fakeFleet) Connected() []string {
	out := make([]string, 0, len(f.members))
	for name := range f.members {
		out = append(out, name)
	}
	if f.missing != "" {
		out = append(out, f.missing)
	}
	sort.Strings(out)
	return out
}

func (f *fakeFleet) ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool) {
	c, ok := f.members[name]
	return c, ok
}

// cachingClient satisfies the transport's client interface over a fake client.
type cachingClient struct {
	client.WithWatch
}

func (cachingClient) AddCacheEventHandler(context.Context, client.Object,
	toolscache.ResourceEventHandler,
) (toolscache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func member(t *testing.T, objs ...client.Object) workloadcluster.SelectivelyCachingClient {
	t.Helper()
	return cachingClient{fake.NewClientBuilder().
		WithScheme(capacityScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
		Build()}
}

// memberRoot is the root a member's own controller creates, carrying the
// capacity it derived from its own hardware.
func memberRoot(chips string) *v1beta1.AcceleratorQuota {
	return &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: rootName},
		Spec:       v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort},
		Status: v1beta1.AcceleratorQuotaStatus{
			Capacity: []v1beta1.AcceleratorCapacityStatus{{
				ResourceName:   "google.com/tpu",
				ResourceFlavor: "tpu7x",
				Allocatable:    resource.MustParse(chips),
			}},
		},
	}
}

func projectingReconciler(t *testing.T, fleet ClusterClients, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	r, c := newReconciler(t, objs...)
	r.Project = ProjectOptions{
		Clusters:      fleet,
		Origin:        hubOrigin,
		FieldManager:  projManager,
		DefaultPolicy: v1beta1.AcceleratorQuotaDistributionProportional,
	}
	return r, c
}

// projectedOn lists what landed on one member.
func projectedOn(t *testing.T, c workloadcluster.SelectivelyCachingClient) map[string]string {
	t.Helper()
	var list v1beta1.AcceleratorQuotaList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list on member: %v", err)
	}
	out := map[string]string{}
	for _, q := range list.Items {
		if q.Name == rootName {
			continue
		}
		nominal := ""
		if len(q.Spec.Budgets) > 0 {
			nominal = q.Spec.Budgets[0].Nominal.String()
		}
		out[q.Name] = nominal
	}
	return out
}

// The whole point of the management plane: one fleet total becomes a number on
// each member, sized to what that member actually has.
func TestProjectSplitsAcrossMembers(t *testing.T) {
	a := member(t, memberRoot("3"))
	b := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-a": a, "member-b": b,
	}}

	r, _ := projectingReconciler(t, fleet,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	// 3:1 capacity, so 90 and 30 — and 120 in total, never more.
	if diff := cmp.Diff(map[string]string{"team": "90"}, projectedOn(t, a)); diff != "" {
		t.Errorf("member-a mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"team": "30"}, projectedOn(t, b)); diff != "" {
		t.Errorf("member-b mismatch (-want +got):\n%s", diff)
	}
}

// Copies come back to the plane that wrote them when a hub is also a member.
// Re-splitting an already-split number compounds it every pass, so a projection
// must never be read as a source.
func TestProjectIgnoresItsOwnCopies(t *testing.T) {
	a := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	// A copy of an earlier pass, sitting on the hub alongside the sources.
	echo := leaf("team", rootName, budget("40"))
	echo.Labels = map[string]string{v1beta1.AcceleratorQuotaOriginLabel: hubOrigin}
	echo.Name = "echoed-team"

	r, _ := projectingReconciler(t, fleet,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
		echo,
	)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	got := projectedOn(t, a)
	if _, echoed := got["echoed-team"]; echoed {
		t.Error("a projection was read back as a source and re-projected")
	}
	if diff := cmp.Diff(map[string]string{"team": "100"}, got); diff != "" {
		t.Errorf("projection mismatch (-want +got):\n%s", diff)
	}
}

// A copy carries the marks that make it recognisable as one: without them a
// member cannot tell it from a node an admin typed, so nothing could ever sweep
// it and the member's webhook would admit edits to it.
func TestProjectMarksItsCopies(t *testing.T) {
	a := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, _ := projectingReconciler(t, fleet,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	var got v1beta1.AcceleratorQuota
	if err := a.Get(context.Background(), client.ObjectKey{Name: "team"}, &got); err != nil {
		t.Fatalf("get projection: %v", err)
	}
	if got.Labels[v1beta1.AcceleratorQuotaOriginLabel] != hubOrigin {
		t.Errorf("origin label = %q, want %q", got.Labels[v1beta1.AcceleratorQuotaOriginLabel], hubOrigin)
	}
	if got.Annotations[v1beta1.AcceleratorQuotaClusterAnnotation] != "member-a" {
		t.Errorf("cluster annotation = %q, want member-a", got.Annotations[v1beta1.AcceleratorQuotaClusterAnnotation])
	}
}

// One member being unreachable, or refusing the write, must cost the fleet that
// member and nothing else. Failing the pass would hold every other member's
// quota at whatever it last received.
func TestProjectIsolatesAFailedMember(t *testing.T) {
	good := member(t, memberRoot("1"))
	broken := cachingClient{fake.NewClientBuilder().
		WithScheme(capacityScheme(t)).
		WithObjects(memberRoot("1")).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object,
				client.Patch, ...client.PatchOption) error {
				return errors.New("apiserver refused the write")
			},
		}).Build()}

	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-good": good, "member-broken": broken,
	}}

	r, _ := projectingReconciler(t, fleet,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)

	// The pass reports the failure...
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err == nil {
		t.Fatal("Reconcile() = nil, want the refused write surfaced")
	}
	// ...and the healthy member still got its share.
	if len(projectedOn(t, good)) == 0 {
		t.Error("a healthy member was left unprojected because another one failed")
	}
}

// A member that has not created its root yet is starting up. Treating that as a
// capacity of zero would apportion it nothing and, worse, would look identical
// to a member that genuinely has no hardware.
func TestProjectWaitsForAMemberThatHasNotReported(t *testing.T) {
	silent := member(t) // no root at all
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": silent}}

	r, _ := projectingReconciler(t, fleet,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	if got := projectedOn(t, silent); len(got) != 0 {
		t.Errorf("a member that reported no capacity was still apportioned: %v", got)
	}
}

// Nothing connected is not a fleet-wide fault. A hub outlives its members'
// restarts, and reporting every node broken while the fleet comes up would bury
// a real failure when one happens.
func TestProjectWithNoMembersConnected(t *testing.T) {
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{}}
	r, _ := projectingReconciler(t, fleet,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Errorf("Reconcile() = %v, want an empty fleet tolerated", err)
	}
}

// A cluster Connected() names but ClientFor no longer has was dropped between
// the two calls. The next pass sees the newer state; this one must not fail.
func TestProjectToleratesAClusterDroppedMidPass(t *testing.T) {
	a := member(t, memberRoot("1"))
	fleet := &fakeFleet{
		members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a},
		missing: "member-vanished",
	}
	r, _ := projectingReconciler(t, fleet,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Errorf("Reconcile() = %v, want a mid-pass drop tolerated", err)
	}
	if len(projectedOn(t, a)) == 0 {
		t.Error("the surviving member was not projected")
	}
}

// registered is a member the admin has told the hub about, whatever its
// current reachability.
func registered(name string) *v1beta1.WorkloadCluster {
	return &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// A member that has dropped out does not merely lose its turn: under a
// proportional split its capacity leaves the basis, so every surviving member's
// share grows to fill the gap -- while the absent member still holds the
// projection it last received. The fleet would then have granted more than the
// admin authorized, and the excess would only surface when the member returned.
func TestProjectHoldsWhenAMemberIsUnreachable(t *testing.T) {
	a := member(t, memberRoot("3"))
	// member-b is registered but the transport has no client for it.
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	// Nothing at all, rather than the 120 member-a would take if the basis
	// silently shrank to the one cluster still answering.
	if got := projectedOn(t, a); len(got) != 0 {
		t.Errorf("projected %v while part of the fleet was unreachable; "+
			"re-splitting without member-b over-grants the fleet", got)
	}
}

// The hold lifts on its own once the fleet is whole again -- it is a wait, not
// a state an operator has to clear.
func TestProjectResumesWhenTheFleetIsWholeAgain(t *testing.T) {
	a := member(t, memberRoot("3"))
	b := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("first Reconcile() = %v", err)
	}
	if len(projectedOn(t, a)) != 0 {
		t.Fatal("the hold did not take")
	}

	fleet.members["member-b"] = b
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("second Reconcile() = %v", err)
	}

	if diff := cmp.Diff(map[string]string{"team": "90"}, projectedOn(t, a)); diff != "" {
		t.Errorf("member-a mismatch after the fleet recovered (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]string{"team": "30"}, projectedOn(t, b)); diff != "" {
		t.Errorf("member-b mismatch after the fleet recovered (-want +got):\n%s", diff)
	}
}

// A member being deregistered is already gone as far as a split is concerned.
// Holding the fleet for one would mean holding until its finalizers ran.
func TestProjectDoesNotHoldForADepartingMember(t *testing.T) {
	a := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	departing := registered("member-b")
	now := metav1.Now()
	departing.DeletionTimestamp = &now
	departing.Finalizers = []string{"ome.io/test-hold"}

	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		departing,
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if len(projectedOn(t, a)) == 0 {
		t.Error("the fleet was held for a member that is being deregistered")
	}
}

// projectionOn builds a copy this plane is meant to own, as an earlier pass
// would have left it.
func projectionOn(name, nominal string) *v1beta1.AcceleratorQuota {
	q := leaf(name, rootName, budget(nominal))
	q.Labels = map[string]string{v1beta1.AcceleratorQuotaOriginLabel: hubOrigin}
	return q
}

// A tenant the tree no longer describes must stop granting quota. Without the
// sweep its queue outlives the budget that justified it, which is a grant
// nobody authorized and nobody can see from the hub.
func TestProjectSweepsWhatTheTreeNoLongerDescribes(t *testing.T) {
	// member-a already carries a copy of a tenant that is not in the tree.
	a := member(t, memberRoot("1"), projectionOn("departed", "40"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	if diff := cmp.Diff(map[string]string{"team": "100"}, projectedOn(t, a)); diff != "" {
		t.Errorf("member-a mismatch (-want +got):\n%s", diff)
	}
}

// The sweep may only remove what this plane wrote. A node an admin authored on
// the member carries no origin, and another plane's copies carry a different
// one -- reaping either would be one plane deleting another's work.
func TestProjectSweepLeavesWhatItDoesNotOwn(t *testing.T) {
	local := leaf("locally-authored", rootName, budget("10"))
	foreign := projectionOn("other-hubs-copy", "20")
	foreign.Labels[v1beta1.AcceleratorQuotaOriginLabel] = "some-other-hub"

	a := member(t, memberRoot("1"), local, foreign)
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	got := projectedOn(t, a)
	for _, name := range []string{"locally-authored", "other-hubs-copy"} {
		if _, kept := got[name]; !kept {
			t.Errorf("the sweep removed %s, which this plane does not own", name)
		}
	}
}

// The subtle one. A frozen node's allowances are withheld, so it is absent from
// what this pass wrote -- and a sweep reading absence as "no longer belongs"
// would delete exactly the copy the freeze exists to preserve, turning a held
// tenant into a deleted one.
func TestProjectSweepKeepsFrozenCopies(t *testing.T) {
	a := member(t, memberRoot("1"), projectionOn("team", "100"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	// A budget above the root's own total freezes the leaf on containment.
	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("50")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	if _, kept := projectedOn(t, a)["team"]; !kept {
		t.Error("a frozen tenant's projection was swept; freezing must hold it, not delete it")
	}
}

// Deleting the node is the one way to take a tenant's quota away on purpose.
// The finalizer is what makes it reach the members at all -- without the claim
// the CR vanishes and its budgets outlive it on every cluster.
func TestProjectReapsOnDeletion(t *testing.T) {
	a := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("first Reconcile() = %v", err)
	}
	if _, projected := projectedOn(t, a)["team"]; !projected {
		t.Fatal("nothing was projected to delete")
	}

	// The claim is what lets deletion reach the members.
	var live v1beta1.AcceleratorQuota
	if err := c.Get(ctx, client.ObjectKey{Name: "team"}, &live); err != nil {
		t.Fatalf("get source: %v", err)
	}
	if len(live.Finalizers) == 0 {
		t.Fatal("the source was not claimed, so a delete would strand its projections")
	}

	if err := c.Delete(ctx, &live); err != nil {
		t.Fatalf("delete source: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("second Reconcile() = %v", err)
	}

	if _, stillThere := projectedOn(t, a)["team"]; stillThere {
		t.Error("the tenant was deleted on the hub but its budget is still live on the member")
	}
}

// The reserved root is the one name that stands on every cluster and is never
// projected: a member's own controller creates it and derives that cluster's
// capacity onto it. Reaping by name alone would take a member's root down with
// the hub's, and the capacity and high-water marks every split is sized against
// would go with it.
func TestProjectDoesNotReapAMembersOwnRoot(t *testing.T) {
	a := member(t, memberRoot("256"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("first Reconcile() = %v", err)
	}

	// Leaf first, then the root, which is the order the delete webhook admits.
	for _, name := range []string{"team", rootName} {
		var live v1beta1.AcceleratorQuota
		if err := c.Get(ctx, client.ObjectKey{Name: name}, &live); err != nil {
			t.Fatalf("get %s on the hub: %v", name, err)
		}
		if err := c.Delete(ctx, &live); err != nil {
			t.Fatalf("delete %s on the hub: %v", name, err)
		}
		if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
			t.Fatalf("Reconcile() after deleting %s = %v", name, err)
		}
	}

	var hub v1beta1.AcceleratorQuota
	if err := c.Get(ctx, client.ObjectKey{Name: rootName}, &hub); !apierrors.IsNotFound(err) {
		t.Fatalf("the hub's own root outlived its delete: err = %v", err)
	}

	var onMember v1beta1.AcceleratorQuota
	if err := a.Get(ctx, client.ObjectKey{Name: rootName}, &onMember); err != nil {
		t.Fatalf("the member's own root was reaped along with the hub's: %v", err)
	}
	if len(onMember.Status.Capacity) == 0 {
		t.Error("the member's derived capacity went with its root")
	}
}

// The reap may only remove what this plane wrote, exactly as the sweep may. A
// grouping with no leaf beneath it never projects, so the name it holds on a
// member belongs to whoever put it there -- and deleting the hub's copy must
// not reach through to an admin's own node or to another plane's.
func TestProjectReapLeavesWhatItDoesNotOwn(t *testing.T) {
	local := cohort("local-group", rootName)
	foreign := cohort("foreign-group", rootName)
	foreign.Labels = map[string]string{v1beta1.AcceleratorQuotaOriginLabel: "some-other-hub"}

	a := member(t, memberRoot("1"), local, foreign)
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("128")),
		cohort("local-group", rootName),
		cohort("foreign-group", rootName),
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("first Reconcile() = %v", err)
	}

	for _, name := range []string{"local-group", "foreign-group"} {
		var live v1beta1.AcceleratorQuota
		if err := c.Get(ctx, client.ObjectKey{Name: name}, &live); err != nil {
			t.Fatalf("get %s on the hub: %v", name, err)
		}
		if err := c.Delete(ctx, &live); err != nil {
			t.Fatalf("delete %s on the hub: %v", name, err)
		}
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	for _, name := range []string{"local-group", "foreign-group"} {
		var onMember v1beta1.AcceleratorQuota
		if err := a.Get(ctx, client.ObjectKey{Name: name}, &onMember); err != nil {
			t.Errorf("the reap removed %s, which this plane does not own: %v", name, err)
		}
	}
}

// What the finalizer uniquely provides, which the sweep cannot: an ordering
// guarantee. The sweep removes a copy once its source leaves the tree, so on
// the happy path deletion would work without any claim at all -- but only
// eventually, and only if this plane keeps running. The finalizer is what stops
// the hub letting go of a node while a member still holds its budget and cannot
// be reached to be told otherwise.
func TestProjectHoldsDeletionWhileAMemberIsUnreachable(t *testing.T) {
	a := member(t, memberRoot("1"), projectionOn("team", "100"))
	// member-b is registered and has no client.
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	source := leaf("team", rootName, budget("100"))
	source.Finalizers = []string{v1beta1.AcceleratorQuotaFinalizer}
	now := metav1.Now()
	source.DeletionTimestamp = &now

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		source,
	)

	// The pass reports that it cannot confirm the budget is gone everywhere.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err == nil {
		t.Fatal("Reconcile() = nil, want the unconfirmable reap surfaced")
	}

	var live v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), client.ObjectKey{Name: "team"}, &live); err != nil {
		t.Fatalf("get source: %v", err)
	}
	if len(live.Finalizers) == 0 {
		t.Error("the hub released the node while a member it could not reach still held the budget")
	}
}

func TestDepthByParentRef(t *testing.T) {
	item := func(name, parent string) v1beta1.AcceleratorQuota {
		q := v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if parent != "" {
			q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
		}
		return q
	}

	tests := []struct {
		name  string
		items []v1beta1.AcceleratorQuota
		want  map[string]int
	}{
		{
			name:  "a chain is measured from the top of the set",
			items: []v1beta1.AcceleratorQuota{item("a", ""), item("b", "a"), item("c", "b")},
			want:  map[string]int{"a": 0, "b": 1, "c": 2},
		},
		{
			// The member's own root is never a projection, so a copy naming it
			// has nothing above it in the swept set.
			name:  "a parent outside the set counts as the top",
			items: []v1beta1.AcceleratorQuota{item("b", "root"), item("c", "b")},
			want:  map[string]int{"b": 0, "c": 1},
		},
		{
			name:  "siblings share a depth",
			items: []v1beta1.AcceleratorQuota{item("a", ""), item("b", "a"), item("c", "a")},
			want:  map[string]int{"a": 0, "b": 1, "c": 1},
		},
		{
			// However it got there, a cycle among a member's objects must not
			// spin the walk. The numbers are meaningless; terminating is not.
			name:  "a cycle terminates",
			items: []v1beta1.AcceleratorQuota{item("a", "b"), item("b", "a")},
			want:  map[string]int{"a": 2, "b": 2},
		},
		{
			name:  "an empty set",
			items: nil,
			want:  map[string]int{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, depthByParentRef(tc.items)); diff != "" {
				t.Errorf("depth mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A node cannot be deleted while it still has children, so a subtree the tree no
// longer describes has to come off a member deepest-first. Parent-first is
// refused at every tier but the last, and the subtree then takes as many passes
// to disappear as it is deep.
func TestProjectSweepsASubtreeChildrenFirst(t *testing.T) {
	// Named so the child sorts before the parent, which is what an order taken
	// from the list rather than from the shape would get wrong.
	parent := projectionOn("sim-tenants", "40")
	parent.Spec.Role = v1beta1.AcceleratorQuotaRoleCohort
	parent.Spec.Budgets = nil
	child := projectionOn("sim-serving", "40")
	child.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: "sim-tenants"}

	var deleted []string
	a := cachingClient{fake.NewClientBuilder().
		WithScheme(capacityScheme(t)).
		WithObjects(memberRoot("1"), parent, child).
		WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.DeleteOption,
			) error {
				deleted = append(deleted, obj.GetName())
				return c.Delete(ctx, obj, opts...)
			},
		}).Build()}
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("100")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	if diff := cmp.Diff([]string{"sim-serving", "sim-tenants"}, deleted); diff != "" {
		t.Errorf("delete order mismatch (-want +got):\n%s", diff)
	}
}

// The failure mode a live fleet hit, and the one the existing hold test could
// not reach.
//
// When a member's apiserver dies, the registry deliberately keeps it connected
// through a grace window -- correct damping for a transport, since a blip
// should not tear down a working client. So Connected() still lists it and
// ClientFor still returns a client; every call through that client simply
// fails. The projector must treat that as "has not reported", because a split
// taken without it hands its share to the survivors while it goes on holding
// the projection it already has.
//
// The pre-existing fake modelled an unreachable member by removing it from both
// Connected() and ClientFor at once, which is the one thing a dead apiserver
// never does.
func TestProjectHoldsWhenAMemberCannotBeRead(t *testing.T) {
	a := member(t, memberRoot("3"))
	b := unreadableMember(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-a": a,
		"member-b": b,
	}}

	r, _ := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	// Nothing at all on the reachable member. 90 would be its correct share of a
	// whole fleet; 120 would be the over-grant. Neither may be written while the
	// basis is short a member.
	if got := projectedOn(t, a); len(got) != 0 {
		t.Errorf("projected %v onto member-a while member-b was unreadable; "+
			"a split taken without member-b over-grants the fleet", got)
	}
}

// unreadableMember is a member whose client exists and whose reads fail, which
// is what a dead apiserver behind a still-connected transport looks like.
func unreadableMember(t *testing.T, objs ...client.Object) workloadcluster.SelectivelyCachingClient {
	t.Helper()
	return cachingClient{fake.NewClientBuilder().
		WithScheme(capacityScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object,
				...client.GetOption,
			) error {
				return errors.New("connection refused")
			},
		}).Build()}
}

// The hub's only account of what its members are doing. Without it the tree
// reports Ready while a member is refusing every write, which is the shape of
// failure this whole component exists to make visible.
func TestProjectReportsEachMemberOnStatus(t *testing.T) {
	a := member(t, memberRoot("3"))
	b := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-a": a,
		"member-b": b,
	}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	var got v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}

	// A row per registered member, in a stable order.
	if diff := cmp.Diff([]string{"member-a", "member-b"}, clusterNames(got.Status.Clusters)); diff != "" {
		t.Errorf("rows mismatch (-want +got):\n%s", diff)
	}
	for _, row := range got.Status.Clusters {
		if row.AppliedGeneration == 0 {
			t.Errorf("%s reports no applied generation, so the hub cannot tell whether its write landed",
				row.Cluster)
		}
		if row.AppliedTime == nil {
			t.Errorf("%s reports no applied time", row.Cluster)
		}
		if row.Message != "" {
			t.Errorf("%s reports %q on a clean pass", row.Cluster, row.Message)
		}
		// Nothing has materialized yet: these members run no controller of
		// their own in this test, so the gap between applied and materialized
		// is the whole point and must not be papered over.
		if row.MaterializedGeneration != 0 {
			t.Errorf("%s claims to have materialized generation %d with no controller to do it",
				row.Cluster, row.MaterializedGeneration)
		}
	}
}

// A member that refused the write is the row an operator needs most, and it
// must say so rather than simply lacking a generation.
func TestProjectReportsAMemberThatRefused(t *testing.T) {
	a := member(t, memberRoot("3"))
	b := refusingMember(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-a": a,
		"member-b": b,
	}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)
	// The pass reports the refusal as an error and still writes status.
	_, _ = r.Reconcile(context.Background(), ctrl.Request{})

	var got v1beta1.AcceleratorQuota
	if err := c.Get(context.Background(), types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	rows := map[string]v1beta1.AcceleratorQuotaClusterStatus{}
	for _, row := range got.Status.Clusters {
		rows[row.Cluster] = row
	}

	if rows["member-b"].Message == "" {
		t.Error("the refusing member reports no message, so its failure is invisible from the hub")
	}
	if rows["member-b"].AppliedGeneration != 0 {
		t.Errorf("the refusing member claims applied generation %d", rows["member-b"].AppliedGeneration)
	}
	// One member's refusal must not cost the others their report.
	if rows["member-a"].AppliedGeneration == 0 {
		t.Error("the healthy member lost its report because another member refused")
	}
}

// Every field written to status needs a matching entry in equalStatus: the
// no-op short-circuit skips the patch when it believes nothing changed, so a
// field it does not compare is written once and frozen. This package has been
// bitten by that twice.
func TestClusterStatusKeepsMoving(t *testing.T) {
	a := member(t, memberRoot("3"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	var got v1beta1.AcceleratorQuota
	if err := c.Get(ctx, types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if got.Status.Clusters[0].Message != "" {
		t.Fatalf("first pass already reports %q", got.Status.Clusters[0].Message)
	}

	// The member starts refusing. Nothing about the node itself changes, so
	// this second pass is exactly the one a missing comparator entry skips --
	// and the hub would go on reporting a member that is now rejecting
	// every write as healthy.
	fleet.members["member-a"] = refusingMember(t, memberRoot("3"))
	_, _ = r.Reconcile(ctx, ctrl.Request{})

	if err := c.Get(ctx, types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if got.Status.Clusters[0].Message == "" {
		t.Error("the member began refusing and the hub still reports it clean -- the report froze")
	}
}

func clusterNames(rows []v1beta1.AcceleratorQuotaClusterStatus) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Cluster)
	}
	return out
}

// refusingMember accepts reads and rejects the projector's applies, which is
// what a member whose webhook or RBAC says no looks like.
func refusingMember(t *testing.T, objs ...client.Object) workloadcluster.SelectivelyCachingClient {
	t.Helper()
	return cachingClient{fake.NewClientBuilder().
		WithScheme(capacityScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.AcceleratorQuota{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object,
				client.Patch, ...client.PatchOption,
			) error {
				return errors.New("denied by the member")
			},
		}).Build()}
}

// The hub's account of what the fleet is consuming, per member and in total.
//
// The numbers come from each member's own copy: only the member can see its
// Kueue, and it has already rolled its queues onto the node they belong to, so
// the hub reads its own arithmetic back beside the consumption that arithmetic
// authorized.
func TestProjectReportsWhatEachMemberIsHolding(t *testing.T) {
	// The members' controllers are not running here, so their copies are seeded
	// with the status a workload-mode manager would have written.
	a := member(t, memberRoot("3"))
	b := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-a": a,
		"member-b": b,
	}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	// Each member now reports consumption against the share it was given.
	reportUsage(t, a, "team", "30", "40", "0")
	reportUsage(t, b, "team", "10", "10", "5")
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	var got v1beta1.AcceleratorQuota
	if err := c.Get(ctx, types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	if len(got.Status.Budgets) != 1 {
		t.Fatalf("hub reported %d budgets, want 1", len(got.Status.Budgets))
	}
	b0 := got.Status.Budgets[0]

	// Nominal is the authored fleet total, not the sum of the shares. They
	// agree when every member reported; when one has not, the split is held,
	// and summing would quietly report a smaller fleet than the admin wrote.
	if b0.Nominal.String() != "120" {
		t.Errorf("fleet nominal = %s, want the authored 120", b0.Nominal.String())
	}
	// Consumption is a fact about the fleet, so it does sum.
	if b0.Admitted.String() != "40" {
		t.Errorf("fleet admitted = %s, want 30+10", b0.Admitted.String())
	}
	if b0.Reserved.String() != "50" {
		t.Errorf("fleet reserved = %s, want 40+10", b0.Reserved.String())
	}
	if b0.Borrowed.String() != "5" {
		t.Errorf("fleet borrowed = %s, want 0+5", b0.Borrowed.String())
	}

	perCluster := map[string]v1beta1.AcceleratorClusterBudgetStatus{}
	for _, pc := range b0.PerCluster {
		perCluster[pc.Cluster] = pc
	}
	if len(perCluster) != 2 {
		t.Fatalf("per-cluster rows = %d, want 2", len(perCluster))
	}
	// The share each member was assigned, read back beside what it is holding:
	// 3:1 reported capacity over a 120 total.
	rowA, rowB := perCluster["member-a"], perCluster["member-b"]
	if got := rowA.Nominal.String(); got != "90" {
		t.Errorf("member-a nominal = %s, want 90", got)
	}
	if got := rowB.Nominal.String(); got != "30" {
		t.Errorf("member-b nominal = %s, want 30", got)
	}
	if got := rowA.Admitted.String(); got != "30" {
		t.Errorf("member-a admitted = %s, want 30", got)
	}
	if got := rowB.Borrowed.String(); got != "5" {
		t.Errorf("member-b borrowed = %s, want 5", got)
	}
}

// The breakdown moves independently of the totals it sums to: two members can
// trade admitted work and leave the fleet figure unchanged. A comparator that
// only looked at the totals would freeze the breakdown after the first write.
func TestPerClusterBudgetsKeepMoving(t *testing.T) {
	a := member(t, memberRoot("1"))
	b := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-a": a,
		"member-b": b,
	}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("128")),
		leaf("team", rootName, budget("120")),
	)
	ctx := context.Background()
	// The projections have to exist before a member can report against them.
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	reportUsage(t, a, "team", "20", "20", "0")
	reportUsage(t, b, "team", "20", "20", "0")
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	// The same fleet total, moved between members.
	reportUsage(t, a, "team", "35", "35", "0")
	reportUsage(t, b, "team", "5", "5", "0")
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	var got v1beta1.AcceleratorQuota
	if err := c.Get(ctx, types.NamespacedName{Name: "team"}, &got); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	for _, pc := range got.Status.Budgets[0].PerCluster {
		if pc.Cluster == "member-a" && pc.Admitted.String() != "35" {
			t.Errorf("member-a admitted = %s after the work moved, want 35 -- the breakdown froze",
				pc.Admitted.String())
		}
	}
	if total := got.Status.Budgets[0].Admitted.String(); total != "40" {
		t.Errorf("fleet admitted = %s, want an unchanged 40", total)
	}
}

// reportUsage writes onto a member's projection what its own workload-mode
// manager would have written after reading its Kueue.
func reportUsage(t *testing.T, m workloadcluster.SelectivelyCachingClient,
	node, admitted, reserved, borrowed string,
) {
	t.Helper()
	ctx := context.Background()
	var q v1beta1.AcceleratorQuota
	if err := m.Get(ctx, types.NamespacedName{Name: node}, &q); err != nil {
		t.Fatalf("reading %s on the member: %v", node, err)
	}
	q.Status.Budgets = []v1beta1.AcceleratorBudgetStatus{{
		ResourceName:   "google.com/tpu",
		ResourceFlavor: "tpu7x",
		Admitted:       resource.MustParse(admitted),
		Reserved:       resource.MustParse(reserved),
		Borrowed:       resource.MustParse(borrowed),
	}}
	if err := m.Status().Update(ctx, &q); err != nil {
		t.Fatalf("writing usage onto %s: %v", node, err)
	}
}

// The hub's whole reason to exist is the fleet-wide view, and a tier that
// reports nothing while the leaves under it run the fleet is the one number an
// operator is most likely to read first.
//
// Only leaves carry figures -- a share is apportioned to a leaf, and an
// ancestor is projected budget-less -- so the tiers above have to be summed to.
func TestFleetUsageRollsUpToAncestors(t *testing.T) {
	a := member(t, memberRoot("2"))
	b := member(t, memberRoot("1"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{
		"member-a": a,
		"member-b": b,
	}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		registered("member-b"),
		cohort(rootName, "", budget("120")),
		cohort("org", rootName, budget("120")),
		leaf("team", "org", budget("120")),
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	reportUsage(t, a, "team", "30", "40", "0")
	reportUsage(t, b, "team", "10", "10", "0")
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	// Every tier reports the same 40, because one tenant's work is the fleet's
	// work however far up you read.
	for _, name := range []string{"team", "org", rootName} {
		var q v1beta1.AcceleratorQuota
		if err := c.Get(ctx, types.NamespacedName{Name: name}, &q); err != nil {
			t.Fatalf("Get(%s) = %v", name, err)
		}
		if len(q.Status.Budgets) == 0 {
			t.Errorf("%s reports no budgets at all", name)
			continue
		}
		if got := q.Status.Budgets[0].Admitted.String(); got != "40" {
			t.Errorf("%s admitted = %s, want 40 -- the leaf's consumption never reached it", name, got)
		}
		if got := q.Status.Budgets[0].Reserved.String(); got != "50" {
			t.Errorf("%s reserved = %s, want 50", name, got)
		}
	}

	ginkgoLikeCheck := func(name string, wantRows int) {
		t.Helper()
		var q v1beta1.AcceleratorQuota
		if err := c.Get(ctx, types.NamespacedName{Name: name}, &q); err != nil {
			t.Fatalf("Get(%s) = %v", name, err)
		}
		if got := len(q.Status.Budgets[0].PerCluster); got != wantRows {
			t.Errorf("%s has %d per-cluster rows, want %d", name, got, wantRows)
		}
	}
	// The breakdown belongs to the node that was actually apportioned a share.
	// An ancestor with per-cluster rows would have to invent a nominal nobody
	// assigned it.
	ginkgoLikeCheck("team", 2)
	ginkgoLikeCheck("org", 0)
	ginkgoLikeCheck(rootName, 0)
}

// Borrowed is the one figure that must not simply add up the tree. A loan
// between two siblings is internal to the tier that contains them, so summing
// their figures would report that tier borrowing from outside itself when it is
// not. Across CLUSTERS it does sum, because a Kueue cohort lives on one member
// and two members lending to the same tenant are two separate loans.
func TestFleetBorrowedIsNotSummedUpTheTree(t *testing.T) {
	a := member(t, memberRoot("2"))
	fleet := &fakeFleet{members: map[string]workloadcluster.SelectivelyCachingClient{"member-a": a}}

	r, c := projectingReconciler(t, fleet,
		registered("member-a"),
		cohort(rootName, "", budget("100")),
		cohort("org", rootName, budget("100")),
		leaf("team", "org", budget("20")),
	)
	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	// The leaf is over its own 20 by 10, borrowed from elsewhere in its cohort.
	reportUsage(t, a, "team", "30", "30", "10")
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	var leafQ, tier v1beta1.AcceleratorQuota
	if err := c.Get(ctx, types.NamespacedName{Name: "team"}, &leafQ); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "org"}, &tier); err != nil {
		t.Fatal(err)
	}
	if got := leafQ.Status.Budgets[0].Borrowed.String(); got != "10" {
		t.Errorf("leaf borrowed = %s, want the backend's own 10", got)
	}
	// The tier is funded for 100 and holds 30, so nothing it runs is above its
	// own allowance -- whatever its child had to borrow to get there.
	if got := tier.Status.Budgets[0].Borrowed.String(); got != "0" {
		t.Errorf("tier borrowed = %s, want 0: it is funded for more than it holds", got)
	}
}
