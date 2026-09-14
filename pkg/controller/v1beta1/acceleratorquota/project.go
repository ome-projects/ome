package acceleratorquota

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/quota/projection"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// ClusterClients is the subset of *workloadcluster.Manager the projector needs.
// *workloadcluster.Manager satisfies it; tests inject a fake.
type ClusterClients interface {
	ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool)
	Connected() []string
}

// ProjectOptions configure the fan-out. Unset disables it, matching the rest of
// the quota plane: a management plane with no origin configured cannot mark what
// it writes, and unmarked copies could never be told from an admin's own nodes.
type ProjectOptions struct {
	// Clusters is the transport. Nil leaves the plane authoring a tree and
	// projecting nothing.
	Clusters ClusterClients

	// Origin identifies this management plane on every copy it writes.
	Origin string

	// FieldManager owns the fields this projector applies on a member.
	FieldManager string

	// DefaultPolicy is the fleet-wide fallback for a budget whose node declares
	// no distribution. Empty leaves such a node unresolved rather than split by
	// a guess.
	DefaultPolicy v1beta1.AcceleratorQuotaDistributionPolicy
}

// Enabled reports whether this manager projects anything.
func (o ProjectOptions) Enabled() bool {
	return o.Clusters != nil && o.Origin != "" && o.FieldManager != ""
}

// clusterState is what one member is doing with one node, as far as this pass
// could tell.
//
// The generations are the point of the type, and they are comparable because
// both count SOURCE revisions: Applied is what the hub wrote, Materialized is
// what the member echoed back after rendering it. Equal means caught up; lower
// means the member has the update and has not acted on it yet. A projection's
// own metadata.generation would answer neither question, being object-local.
type clusterState struct {
	appliedGeneration      int64
	appliedTime            *metav1.Time
	materializedGeneration int64
	message                string

	// budgets is this member's numbers for each (resource, flavor) the node
	// budgets, keyed by budgetKey.
	budgets map[string]clusterBudget
}

// clusterBudget is what one member was given and what it is doing with it.
//
// Nominal comes from the hub's own split, the rest from the member: it is the
// only party that can see its Kueue, and it publishes what it sees on the copy
// the hub projected. So the hub reads its own arithmetic back alongside the
// consumption that arithmetic authorized.
type clusterBudget struct {
	// The pair these figures describe, carried rather than parsed back out of
	// the map key: a resource name is an arbitrary qualified name and splitting
	// a rendered key would be a guess about what may appear inside one.
	resourceName   string
	resourceFlavor string

	nominal  resource.Quantity
	admitted resource.Quantity
	reserved resource.Quantity
	borrowed resource.Quantity
}

// fleetView is every node's state on every registered member.
//
// Registered, not connected: the member an operator most needs a row for is the
// one that is missing, and a view that listed only who answered would drop it
// silently -- the same shape of mistake as a split taken over whoever answered.
type fleetView map[string]map[string]clusterState

// observe records one member's numbers for one budget on one node.
func (v fleetView) observe(node, cluster, key string, mutate func(*clusterBudget)) {
	v.note(node, cluster, func(st *clusterState) {
		if st.budgets == nil {
			st.budgets = map[string]clusterBudget{}
		}
		b := st.budgets[key]
		mutate(&b)
		st.budgets[key] = b
	})
}

func (v fleetView) note(node, cluster string, mutate func(*clusterState)) {
	if v[node] == nil {
		v[node] = map[string]clusterState{}
	}
	st := v[node][cluster]
	mutate(&st)
	v[node][cluster] = st
}

// say records a message for one node across every listed member, for the
// verdicts that are the hub's own and identical everywhere: a held fleet, an
// unresolved split, a frozen node.
func (v fleetView) say(node string, clusters []string, message string) {
	for _, c := range clusters {
		v.note(node, c, func(st *clusterState) { st.message = message })
	}
}

// project fans the authored tree out across the fleet.
//
// One pass: read what each member says it has, split every budget against that,
// and apply each member's copy. Members are independent — a cluster that is
// unreachable, or that refuses a write, costs the fleet that cluster and
// nothing else. The alternative, failing the pass, would let one broken member
// hold every other member's quota at whatever it last received.
//
// Returns the per-node outcomes the caller puts on status, and an error only
// when the pass could not be attempted at all.
func (r *Reconciler) project(ctx context.Context, built *tree.Tree,
	frozen map[string]tree.Violation,
) (fleetView, error) {
	log := r.Log.WithName("project")
	view := fleetView{}
	connected := r.Project.Clusters.Connected()
	sort.Strings(connected)
	if len(connected) == 0 {
		// Nothing is reachable yet. Not a failure: a hub outlives its members'
		// restarts, and reporting every node broken because the fleet is still
		// coming up would bury a real fault when one happens.
		log.V(1).Info("no member is connected, so nothing is projected this pass")
		return view, nil
	}

	// Every member the admin registered, not merely the ones answering. A
	// proportional split divides by the fleet's capacity, so a member that has
	// dropped out does not simply lose its turn — its capacity leaves the basis
	// and every surviving member's share grows to fill the gap. Meanwhile the
	// absent member still holds the projection it last received, so the fleet
	// would have handed out more than the admin authorized, and the excess would
	// only become visible when the member came back.
	//
	// So the pass holds instead. Freezing keeps every projection exactly where
	// it is, which is wrong by at most the change an admin just made; rebalancing
	// is wrong by the whole of the missing member's share. An operator resolves
	// it by fixing the member or deregistering it, and either way says what they
	// meant.
	registered, err := r.registeredClusters(ctx)
	if err != nil {
		return view, fmt.Errorf("reading the cluster registry: %w", err)
	}
	if absent := missing(registered, connected); len(absent) > 0 {
		log.Info("holding every projection: part of the fleet is unreachable",
			"unreachable", absent, "connected", connected)
		reason := fmt.Sprintf(
			"projections held: %v unreachable, and re-splitting without them would over-grant the fleet", absent)
		for _, node := range built.Nodes() {
			view.say(node.Name(), registered, reason)
		}
		return view, nil
	}

	capacity, reported, unreadable := r.fleetCapacity(ctx, connected)
	for cluster, err := range unreadable {
		// Logged, and also acted on: a member missing from Reported holds every
		// proportional split below. Explicit shares are unaffected -- they are
		// the admin's arithmetic and need no reading.
		log.Info("member capacity unavailable, so proportional splits are held",
			"cluster", cluster, "reason", err.Error())
	}

	resolution := projection.Resolve(built, projection.Fleet{
		Registered: registered,
		Reported:   reported,
		Capacity:   capacity,
	}, projection.ResolveOptions{
		DefaultPolicy: r.Project.DefaultPolicy,
	})

	for node, reason := range resolution.Unresolved {
		view.say(node, registered, reason)
	}

	// A frozen node keeps whatever it last received. Withholding this pass's
	// copy IS the freeze: the member's existing projection is left exactly as it
	// is, rather than being rewritten from a tree the hub has already judged
	// unsound. Dropping the leaf also drops any ancestor only it needed, since
	// the matched set is derived from the leaves that travel.
	for cluster, allowances := range resolution.ByCluster {
		kept := allowances[:0]
		for _, a := range allowances {
			if v, held := frozen[a.Node]; held {
				view.say(a.Node, registered, fmt.Sprintf("projection held at its last value: %s", v.Message))
				continue
			}
			kept = append(kept, a)
		}
		resolution.ByCluster[cluster] = kept
	}

	var errs []error
	for _, cluster := range connected {
		if err := r.projectOnto(ctx, built, cluster, resolution.ByCluster[cluster], frozen, view); err != nil {
			// Recorded per cluster and carried on. The next member's quota does
			// not depend on this one's write having landed.
			errs = append(errs, fmt.Errorf("cluster %s: %w", cluster, err))
		}
	}
	if len(errs) > 0 {
		return view, errors.Join(errs...)
	}
	return view, nil
}

// registeredClusters is every member an admin has registered, whatever its
// current reachability. Read from the hub's own registry rather than from the
// transport, which by construction knows only what it could connect to.
func (r *Reconciler) registeredClusters(ctx context.Context) ([]string, error) {
	var list v1beta1.WorkloadClusterList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(list.Items))
	for i := range list.Items {
		// A member on its way out is already gone as far as a split is
		// concerned: its projections are being reaped, and holding the fleet
		// for it would mean holding until its finalizers ran.
		if !list.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		out = append(out, list.Items[i].Name)
	}
	sort.Strings(out)
	return out, nil
}

// missing names the registered clusters the transport has no client for.
func missing(registered, connected []string) []string {
	live := make(map[string]struct{}, len(connected))
	for _, c := range connected {
		live[c] = struct{}{}
	}
	var out []string
	for _, name := range registered {
		if _, ok := live[name]; !ok {
			out = append(out, name)
		}
	}
	return out
}

// projectOnto applies one member's copy of the tree, then removes the copies
// that no longer belong there.
func (r *Reconciler) projectOnto(ctx context.Context, built *tree.Tree,
	cluster string, allowances []projection.Allowance, frozen map[string]tree.Violation,
	view fleetView,
) error {
	remote, ok := r.Project.Clusters.ClientFor(cluster)
	if !ok {
		// Connected() said it was there and ClientFor disagrees, which means it
		// dropped between the two. The next pass sees the newer state.
		return nil
	}

	// The hub's half of the record: what it decided this member should have.
	// Kept even when the apply below fails, because "assigned 40 and could not
	// write it" is a different report from "assigned nothing".
	for _, a := range allowances {
		nominal := a.Nominal.DeepCopy()
		name, flavor := a.ResourceName, a.ResourceFlavor
		view.observe(a.Node, cluster, budgetKey(name, flavor), func(b *clusterBudget) {
			b.resourceName, b.resourceFlavor = name, flavor
			b.nominal = nominal
		})
	}

	objects, err := projection.For(built, cluster, allowances, projection.Options{Origin: r.Project.Origin})
	if err != nil {
		return fmt.Errorf("rendering: %w", err)
	}

	var errs []error
	for _, obj := range objects {
		if err := setKind(obj, r.Scheme); err != nil {
			errs = append(errs, err)
			continue
		}
		// Force ownership because this projector is the declared author of a
		// projection: the copy exists only because it wrote it, so a field some
		// other manager has taken over is drift to correct rather than a claim
		// to respect. That is the opposite of the materializer's stance towards
		// hand-authored Kueue objects, which it refuses to adopt.
		if err := remote.Patch(ctx, obj, client.Apply,
			client.FieldOwner(r.Project.FieldManager), client.ForceOwnership); err != nil {
			errs = append(errs, fmt.Errorf("applying %s: %w", obj.Name, err))
			view.note(obj.Name, cluster, func(st *clusterState) { st.message = err.Error() })
			continue
		}
		// Applied, which is not the same as materialized: the member has the
		// revision, and its own controller renders it on its own schedule. The
		// gap between these two numbers is exactly that lag.
		applied := sourceGenerationOf(obj)
		now := metav1.Now()
		view.note(obj.Name, cluster, func(st *clusterState) {
			st.appliedGeneration = applied
			st.appliedTime = &now
			st.message = ""
		})
	}

	// One read, two uses. The sweep needs every copy this plane owns here, and
	// so does the read-back -- listing twice would double the cost of a pass for
	// nothing, and could see two different states.
	live, err := r.projectionsOn(ctx, remote, cluster)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	r.readBack(cluster, live, view)

	// Applied first, swept second. A node that moved between clusters exists
	// briefly on both, and sweeping first would delete the copy this pass is
	// about to write.
	if err := r.sweepMember(ctx, remote, cluster, live, keepOn(objects, frozen)); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// projectionsOn is every copy this plane owns on one member.
func (r *Reconciler) projectionsOn(ctx context.Context, remote client.Client, cluster string,
) ([]v1beta1.AcceleratorQuota, error) {
	var live v1beta1.AcceleratorQuotaList
	if err := remote.List(ctx, &live, client.MatchingLabels{
		v1beta1.AcceleratorQuotaOriginLabel: r.Project.Origin,
	}); err != nil {
		return nil, fmt.Errorf("listing projections on %s: %w", cluster, err)
	}
	return live.Items, nil
}

// readBack takes what each copy reports about itself onto the hub's view.
//
// Read from the copy rather than asked of the member's Kueue directly: the hub
// holds no Kueue credential on a member and should not, and the member has
// already done the work of rolling its queues up onto the node they belong to.
func (r *Reconciler) readBack(cluster string, live []v1beta1.AcceleratorQuota, view fleetView) {
	for i := range live {
		obj := &live[i]
		// What the member has actually rendered, which is not what it was last
		// sent. The two are compared on status, so reporting the sent number
		// here would make every member look permanently caught up.
		view.note(obj.Name, cluster, func(st *clusterState) {
			st.materializedGeneration = obj.Status.SourceGeneration
		})
		for _, b := range obj.Status.Budgets {
			admitted, reserved, borrowed := b.Admitted.DeepCopy(), b.Reserved.DeepCopy(), b.Borrowed.DeepCopy()
			name, flavor := b.ResourceName, b.ResourceFlavor
			view.observe(obj.Name, cluster, budgetKey(name, flavor), func(cb *clusterBudget) {
				cb.resourceName, cb.resourceFlavor = name, flavor
				cb.admitted, cb.reserved, cb.borrowed = admitted, reserved, borrowed
			})
		}
	}
}

// keepOn is what may remain on a member: what this pass wrote, plus the copies
// belonging to frozen nodes.
//
// Frozen nodes are the subtle half. Their allowances are withheld, so they are
// absent from what was written — and a sweep that read absence as "no longer
// belongs" would delete exactly the projections the freeze exists to preserve,
// turning a held tenant into a deleted one.
func keepOn(written []*v1beta1.AcceleratorQuota, frozen map[string]tree.Violation) sets.Set[string] {
	keep := sets.New[string]()
	for _, obj := range written {
		keep.Insert(obj.Name)
	}
	for name := range frozen {
		keep.Insert(name)
	}
	return keep
}

// sourceGenerationOf reads the generation the renderer stamped on a copy. It is
// the same annotation the member echoes into status.sourceGeneration once it has
// materialized, which is what makes the two numbers comparable.
func sourceGenerationOf(obj *v1beta1.AcceleratorQuota) int64 {
	n, err := strconv.ParseInt(obj.Annotations[v1beta1.AcceleratorQuotaSourceGenerationAnnotation], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// depthByParentRef measures how far each node sits below the top of the set,
// following parentRef. A name whose parent is absent counts as depth 0: the
// parent is either the member's own root or already gone, and either way there
// is nothing above it here to delete first.
//
// The walk is bounded by the number of items rather than by reaching a root.
// These objects come off a member, and a cycle among them — however it got
// there — must not spin this loop.
func depthByParentRef(items []v1beta1.AcceleratorQuota) map[string]int {
	parent := make(map[string]string, len(items))
	for i := range items {
		if ref := items[i].Spec.ParentRef; ref != nil {
			parent[items[i].Name] = ref.Name
		}
	}
	present := make(map[string]bool, len(items))
	for i := range items {
		present[items[i].Name] = true
	}
	depth := make(map[string]int, len(items))
	for i := range items {
		name, d := items[i].Name, 0
		for d < len(items) {
			p, ok := parent[name]
			if !ok || !present[p] {
				break
			}
			name, d = p, d+1
		}
		depth[items[i].Name] = d
	}
	return depth
}

// sweepMember deletes the copies this plane owns on one member that no longer
// belong there.
//
// Scoped by the origin label, so it can only ever remove what this management
// plane wrote: a node an admin authored on the member carries no origin, and
// another plane's copies carry a different one. Both are invisible here, which
// is what lets two planes share a member without reaping each other.
//
// This is what makes a tenant deletable at all, and what removes a copy from a
// cluster a leaf no longer matches — a share that fell to zero leaves a queue
// behind that would otherwise keep granting quota nobody authorized.
func (r *Reconciler) sweepMember(ctx context.Context, remote client.Client,
	cluster string, live []v1beta1.AcceleratorQuota, keep sets.Set[string],
) error {
	// Deepest first, the reverse of the order they were applied in: a node is
	// refused deletion while it still has children, so removing a whole subtree
	// parent-first is denied at every tier but the last and takes as many passes
	// as the subtree is deep.
	doomed := make([]*v1beta1.AcceleratorQuota, 0, len(live))
	for i := range live {
		obj := &live[i]
		if keep.Has(obj.Name) || !obj.DeletionTimestamp.IsZero() {
			continue
		}
		doomed = append(doomed, obj)
	}
	depth := depthByParentRef(live)
	sort.Slice(doomed, func(i, j int) bool {
		if di, dj := depth[doomed[i].Name], depth[doomed[j].Name]; di != dj {
			return di > dj
		}
		return doomed[i].Name < doomed[j].Name
	})

	var errs []error
	for _, obj := range doomed {
		// Guarded on the resourceVersion read a moment ago, so a copy this pass
		// is concurrently rewriting is not deleted out from under itself.
		if err := remote.Delete(ctx, obj, client.Preconditions{
			UID:             &obj.UID,
			ResourceVersion: &obj.ResourceVersion,
		}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			errs = append(errs, fmt.Errorf("reaping %s on %s: %w", obj.Name, cluster, err))
		}
	}
	return errors.Join(errs...)
}

// reapProjections removes a departing node's copies from every member.
//
// The explicit-deletion path, and the only one that takes a tenant's quota away
// on purpose. A freeze holds a projection; a sweep removes what the tree no
// longer describes; this is what happens when an admin says to delete the node
// itself.
//
// Scoped by the origin label, exactly as the sweep is, because a name is not
// proof of authorship. The reserved root stands on every member under the same
// name and is that member's own — never projected, its capacity being the one
// thing only the member can derive — so reaping by name alone takes a member's
// root down with the hub's, and its capacity and high-water marks with it.
//
// Every member must answer before the finalizer is released. A member that is
// unreachable keeps the node alive on the hub rather than letting it go with a
// live budget still standing somewhere — which is the same reasoning as holding
// the split during a partition, applied to deletion.
func (r *Reconciler) reapProjections(ctx context.Context, name string) error {
	var errs []error
	for _, cluster := range r.Project.Clusters.Connected() {
		remote, ok := r.Project.Clusters.ClientFor(cluster)
		if !ok {
			continue
		}
		if err := r.reapProjectionOn(ctx, remote, cluster, name); err != nil {
			errs = append(errs, err)
		}
	}

	registered, err := r.registeredClusters(ctx)
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("reading the cluster registry: %w", err))...)
	}
	if absent := missing(registered, r.Project.Clusters.Connected()); len(absent) > 0 {
		errs = append(errs, fmt.Errorf(
			"cannot confirm %s is gone from %v: unreachable, and releasing the node would leave a live budget there",
			name, absent))
	}
	return errors.Join(errs...)
}

// reapProjectionOn removes one node's copy from one member, when that copy is
// this plane's to remove.
//
// The ownership read is metadata-only, and that is what makes it a LIVE read
// rather than a convenience: the transport serves AcceleratorQuota from a
// background cache, and a cached miss here would read as "nothing to reap" and
// release the finalizer over a copy still standing on the member.
// PartialObjectMetadata bypasses that cache, and carries everything the check
// needs.
func (r *Reconciler) reapProjectionOn(ctx context.Context, remote client.Client,
	cluster, name string,
) error {
	var existing metav1.PartialObjectMetadata
	existing.SetGroupVersionKind(acceleratorQuotaGVK)
	if err := remote.Get(ctx, types.NamespacedName{Name: name}, &existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("reading %s on %s: %w", name, cluster, err)
	}
	if existing.Labels[v1beta1.AcceleratorQuotaOriginLabel] != r.Project.Origin {
		return nil
	}

	// Guarded on the UID rather than the resourceVersion the sweep uses: a
	// member's own controller restamps status on a copy continuously, so a
	// resourceVersion read a moment ago would refuse the delete over a write
	// that changed nothing this check depends on. Identity is what is worth
	// pinning — a copy deleted and recreated between the two calls is a
	// different object, and not the one this check cleared.
	obj := &v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := remote.Delete(ctx, obj, client.Preconditions{UID: &existing.UID}); err != nil &&
		!apierrors.IsNotFound(err) {
		return fmt.Errorf("reaping %s on %s: %w", name, cluster, err)
	}
	return nil
}

// fleetCapacity reads what each member says it has.
//
// From each member's own reserved root, which its local controller derives from
// the hardware actually there. The hub measures nothing: it holds no credential
// to read a member's nodes, and the grant it does hold covers quota objects
// only. A member that has not reported yet is simply absent from the result,
// which a proportional split reads as "wait", not as "zero".
func (r *Reconciler) fleetCapacity(ctx context.Context, clusters []string,
) ([]projection.Capacity, []string, map[string]error) {
	var out []projection.Capacity
	var reported []string
	unreadable := map[string]error{}

	for _, cluster := range clusters {
		remote, ok := r.Project.Clusters.ClientFor(cluster)
		if !ok {
			// Connected() said it was there and ClientFor disagrees, so it
			// dropped between the two. Not reported: a member that cannot be
			// asked has not answered, and letting it fall out silently is what
			// shrinks the basis.
			unreadable[cluster] = errors.New("no client for this member")
			continue
		}
		var root v1beta1.AcceleratorQuota
		if err := remote.Get(ctx, types.NamespacedName{Name: r.Options.RootName}, &root); err != nil {
			// Including NotFound. A member that has not created its root yet is
			// starting up rather than broken, but it still has not reported, and
			// a split taken without it hands its share to everyone else.
			unreadable[cluster] = err
			continue
		}

		// Answered. That is recorded even when the member holds none of the
		// budgeted flavors: reporting nothing is a reading, and it apportions
		// that member zero rather than removing it from the basis.
		reported = append(reported, cluster)
		for _, c := range root.Status.Capacity {
			out = append(out, projection.Capacity{
				Cluster:        cluster,
				ResourceName:   c.ResourceName,
				ResourceFlavor: c.ResourceFlavor,
				Allocatable:    c.Allocatable.DeepCopy(),
				HighWaterMark:  c.HighWaterMark.DeepCopy(),
				ObservedAt:     c.ObservedAt.DeepCopy(),
			})
		}
	}
	return out, reported, unreadable
}

// authoredHere drops the copies other planes wrote, keeping only what an admin
// authored on this cluster.
//
// The second of the loop guards, and the one that would bite hardest without it.
// A management plane that is also a member — a single cluster running both
// halves, or a hub someone registered against itself — reads its own projections
// back through the same lister. Re-splitting an already-split number would
// compound it every pass, and the copies would then be projected as sources onto
// every other member.
func authoredHere(items []v1beta1.AcceleratorQuota) []v1beta1.AcceleratorQuota {
	out := make([]v1beta1.AcceleratorQuota, 0, len(items))
	for _, q := range items {
		if _, projected := q.Labels[v1beta1.AcceleratorQuotaOriginLabel]; projected {
			continue
		}
		out = append(out, q)
	}
	return out
}

// acceleratorQuotaGVK is what a metadata-only read has to be told it is asking
// for. A PartialObjectMetadata is the same Go type whatever it holds, so the
// kind cannot be inferred from it and the client routes on what is stamped here.
var acceleratorQuotaGVK = v1beta1.SchemeGroupVersion.WithKind("AcceleratorQuota")

// setKind stamps the object's GroupVersionKind. A server-side apply carries no
// kind of its own — the apiserver reads it off the payload — so an object built
// in memory rather than decoded from the wire has to be told what it is.
func setKind(obj *v1beta1.AcceleratorQuota, s *runtime.Scheme) error {
	gvks, _, err := s.ObjectKinds(obj)
	if err != nil {
		return fmt.Errorf("resolving kind: %w", err)
	}
	if len(gvks) == 0 {
		return fmt.Errorf("no kind registered for AcceleratorQuota")
	}
	obj.SetGroupVersionKind(gvks[0])
	return nil
}
