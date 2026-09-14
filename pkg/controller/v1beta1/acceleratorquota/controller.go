// Package acceleratorquota reconciles the AcceleratorQuota tree's observed
// state. It assembles every node on the cluster, re-runs the invariant checks
// the admission webhook runs, and writes each node's position and condition.
//
// It materializes nothing. Admission is fast feedback but not a serialization
// point — two children admitted concurrently can each pass containment and
// together bust their parent, and a parent can be deleted out from under a
// child — so the tree has to be re-checked by something that sees it whole,
// after the fact. That re-check is this controller's whole job for now; the
// Kueue materializer and the management-plane projector build on the same
// assembled tree.
package acceleratorquota

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/backend"
	"sigs.k8s.io/ome/pkg/quota/tree"
	"sigs.k8s.io/ome/pkg/quota/usage"
)

// Mode selects which half of the quota plane a manager runs. It is a manager
// flag rather than config because it decides which controllers exist.
type Mode string

const (
	// ModeWorkload renders the local tree into Kueue on a serving cluster. It is
	// the only mode that writes Kueue objects, and only on its own cluster.
	ModeWorkload Mode = "workload"

	// ModeManagement holds the authored fleet tree and projects per-cluster
	// shares onto members. It writes no Kueue object anywhere.
	ModeManagement Mode = "management"
)

// Deliberately no +kubebuilder:rbac markers here, unlike every sibling in this
// directory. controller-gen scans ./pkg/controller/... into the ome-manager
// ClusterRole, but this controller runs in ome-quota-manager, a separate binary
// with its own ServiceAccount. A marker added here would silently hand
// ome-manager the quota plane's permissions — today AcceleratorQuota, later
// cluster-wide write on Kueue's ClusterQueue and Cohort — and that write grant
// belongs only to the process that renders Kueue, not to the one also running
// the InferenceService reconcilers and the fail-closed pod mutating webhook.
//
// The grants live in charts/ome-quota-manager/templates/rbac.yaml, and
// charts/ome-resources/tests/render_test.sh fails if they ever appear in the
// ome-manager role.

// Reconciler keeps every AcceleratorQuota's status in step with the tree they
// collectively describe.
//
// Reconciliation is whole-tree, not per-object: one node's edit can change
// another node's verdict — re-parenting moves a subtree's paths, and a budget
// change can bust a parent that was fine a moment ago — so every pass rebuilds
// the tree and reconciles every node's status. A fleet is a handful of nodes,
// so this costs one LIST.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// APIReader is an uncached reader. The tree verdict must not be computed
	// from a stale cache: a node admitted a moment ago but not yet in the
	// informer cache would be invisible, and this controller would then clear a
	// Degraded condition that is still true.
	APIReader client.Reader

	// Options carry the operator-configured root name and depth bound. The
	// package invents neither.
	Options tree.Options

	// ResyncInterval re-runs the checks on an otherwise-idle plane, so a
	// violation only a clock can clear is noticed without an edit. Zero leaves
	// reconciles purely event-driven.
	ResyncInterval time.Duration

	// Capacity configures deriving this cluster's own accelerator capacity onto
	// the reserved root. Unset leaves the controller reporting tree position
	// only, which is what a management-mode manager wants: it holds the
	// authored fleet tree and has no local silicon to measure.
	Capacity CapacityOptions

	// Project configures fanning the authored tree out across a fleet. Unset is
	// the workload-mode shape: a member holds no transport, which is the
	// structural half of the loop guard — a projection has nothing to be
	// re-projected by.
	Project ProjectOptions

	// Materialize configures rendering the tree into an enforcement backend.
	// Unset leaves the tree observed but unenforced, which is both the
	// management-mode shape and the way a workload cluster runs before an
	// operator opts into writing Kueue objects.
	Materialize MaterializeOptions

	// Mode is which half of the plane this manager runs, carried here only so
	// the budget metrics can say which. The option structs above already encode
	// the behavioural difference; this names it for a reader of the series,
	// where the same metric means the authored fleet total in one mode and one
	// cluster's share in the other.
	Mode Mode
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("acceleratorquota", req.Name)

	var list v1beta1.AcceleratorQuotaList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing AcceleratorQuotas: %w", err)
	}

	// A node being deleted is not part of the tree it is leaving. Excluding it
	// keeps this pass from re-rendering the objects the finalizer is about to
	// reap, which would read as a failed reap.
	live, deleting := partitionByDeletion(list.Items)

	// A management plane reads only what an admin wrote here. Its own copies can
	// come back to it — a hub registered against itself, or one cluster running
	// both halves — and re-splitting an already-split number would compound it
	// every pass. A member does the opposite: projections ARE its tree, so this
	// filter must not run there.
	if r.Project.Enabled() {
		live = authoredHere(live)
	}

	built, violations, err := tree.Build(live, r.Options)
	if err != nil {
		// A configuration failure, not a property of the nodes. Surfacing it as
		// an error requeues with backoff and logs, rather than silently leaving
		// every node's status untouched as if there were nothing to do.
		return ctrl.Result{}, fmt.Errorf("assembling the quota tree: %w", err)
	}

	// Capacity is cluster state, so the pure tree checks cannot see it. The
	// verdict joins them here rather than in a path of its own, so a budget the
	// fleet cannot afford freezes and reports exactly like any other violated
	// invariant. The numbers come from the root's last observed capacity: this
	// pass has not derived new ones yet, and a value one pass old is the right
	// input for a check whose whole purpose is to ignore momentary dips.
	if r.Capacity.Enabled() && built.Root != nil {
		violations = append(violations,
			capacityViolations(built.Nodes(), built.Root.Quota.Status.Capacity)...)
	}

	frozen := built.Frozen(violations)
	var errs []error

	// Claimed whenever this plane writes something a delete has to take back:
	// Kueue objects here, or projections on members. Without the claim the CR
	// vanishes on delete and whatever it wrote outlives it, unreferenced.
	if r.Materialize.Enabled() || r.Project.Enabled() {
		// Claim every live node before writing anything, so a delete that
		// arrives mid-pass still finds a finalizer to hold it.
		for i := range live {
			if err := r.ensureFinalizer(ctx, &live[i]); err != nil {
				errs = append(errs, fmt.Errorf("claiming node %s: %w", live[i].Name, err))
			}
		}
		// Reap before applying. A departing node and a live one can name the
		// same object transiently, and reaping second would delete what this
		// pass just wrote.
		keep := sets.New[string]()
		for _, node := range built.Nodes() {
			keep.Insert(node.Name())
		}
		for i := range deleting {
			if err := r.finalize(ctx, &deleting[i], keep); err != nil {
				errs = append(errs, fmt.Errorf("releasing node %s: %w", deleting[i].Name, err))
			}
		}
	}

	outcomes := map[string]backend.NodeOutcome{}
	if r.Materialize.Enabled() && len(errs) == 0 {
		outcomes, err = r.materialize(ctx, built, frozen)
		if err != nil {
			errs = append(errs, err)
		}
	}

	var fleet fleetView
	if r.Project.Enabled() {
		var err error
		fleet, err = r.project(ctx, built, frozen)
		if err != nil {
			// Per-cluster failures are already folded in here; this is the
			// remainder. Collected rather than returned so the status writes
			// below still happen — an operator needs the tree's verdict most
			// when a member is refusing it.
			errs = append(errs, fmt.Errorf("projecting the quota tree: %w", err))
		}
	}

	// What the fleet is holding, summed up the tree. Only leaves carry figures
	// -- a share is apportioned to a leaf and an ancestor travels budget-less --
	// so without this a grouping tier and the root report nothing while the
	// leaves beneath them run the whole fleet.
	//
	// The same roll the workload plane uses on its own queues, rather than a
	// second one here: an ancestor's borrowed figure is recomputed rather than
	// summed, because a loan between two siblings is internal to the subtree
	// that contains them and adding the children's numbers would report the
	// subtree borrowing from outside itself.
	fleetRollup := usage.Roll(built, fleetLeafUsage(fleet))

	// Observational, so a backend that cannot be read costs the fleet its usage
	// figures for this pass and nothing else. Failing here would stop the
	// conditions and tree positions below from being written too, which is the
	// half an operator actually acts on.
	reading, err := r.rollUsage(ctx, built)
	if err != nil {
		log.V(1).Info("consumption not read this pass", "reason", err.Error())
	}

	inTree := make(map[string]struct{}, len(built.Nodes()))
	for _, node := range built.Nodes() {
		inTree[node.Name()] = struct{}{}
		if err := r.reconcileStatus(ctx, node, frozen, outcomes[node.Name()], reading,
			fleet[node.Name()], fleetRollup[node.Name()]); err != nil {
			errs = append(errs, fmt.Errorf("writing status for node %s: %w", node.Name(), err))
		}
	}
	// The pass holds the authoritative node set, so a node that left the tree
	// without going through its finalizer -- force-stripped, or deleted while
	// this manager was down -- loses its series here rather than reporting a
	// deleted tenant's accelerators forever.
	sweepBudgets(inTree)
	if len(errs) > 0 {
		// One node's failure must not skip the rest, so they are collected and
		// the whole pass retried. Each carries the stage it came from: claiming,
		// releasing and status writing touch different verbs on different
		// resources, and one prefix over all three sends a reader chasing the
		// wrong permission.
		return ctrl.Result{}, fmt.Errorf("reconciling the quota tree: %w", errors.Join(errs...))
	}

	if len(violations) > 0 {
		log.V(1).Info("quota tree has violations", "count", len(violations), "nodes", violations.Nodes())
	}

	// Capacity describes the cluster as a whole, so it needs the node that means
	// "the whole cluster". Create it rather than wait: a cluster reports what it
	// has whether or not anyone has authored a budget yet.
	if r.Capacity.Enabled() {
		if built.Root == nil {
			created, err := r.ensureRoot(ctx)
			if err != nil {
				if !webhookUnavailable(err) {
					return ctrl.Result{}, err
				}
				// The startup pass makes the same create and already treats this
				// as a wait; surfacing it here as well would answer a condition
				// that clears on its own with a stack trace per attempt.
				log.V(1).Info("waiting to create the reserved root",
					"root", r.Options.RootName, "reason", err.Error())
				return ctrl.Result{RequeueAfter: rootBootstrapRetry}, nil
			}
			if !created {
				// The name is taken by a node that is not the tree root: it
				// carries a parentRef, or sits in a cycle. Creating cannot fix
				// that and nor can retrying, so without saying so capacity
				// derivation would sit idle forever with no error, no event and
				// no condition to explain it.
				log.Info("capacity derivation is idle: the reserved root name is held by a node that is not the tree root",
					"root", r.Options.RootName)
			}
			// Keep the resync. The create's own watch event normally brings us
			// straight back, but in the idle case above there is no such event
			// and dropping the interval would disable the only thing that would
			// notice the tree being repaired.
			return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
		}
		if err := r.reconcileCapacity(ctx, built.Root.Quota); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: r.ResyncInterval}, nil
}

// reconcileStatus writes one node's observed position and condition. It patches
// only when something changed, so an idle fleet does not rewrite status on every
// resync tick and spin the watch.
func (r *Reconciler) reconcileStatus(ctx context.Context, node *tree.Node, frozen map[string]tree.Violation,
	outcome backend.NodeOutcome, reading usageReading,
	onClusters map[string]clusterState, rolled map[usage.Key]usage.Total,
) error {
	current := node.Quota
	updated := current.DeepCopy()

	updated.Status.ObservedGeneration = current.Generation
	updated.Status.Parent = ""
	if node.Parent != nil {
		updated.Status.Parent = node.Parent.Name()
	}
	// Only rewritten when consumption was actually read. A pass that could not
	// reach the backend leaves the previous figures standing rather than
	// blanking them: "unknown" and "nothing held" are different answers, and
	// only one of them is safe to publish on a failed read.
	if totals, ok := reading.forNode(node.Name()); ok {
		updated.Status.Budgets = budgetStatus(node, totals)
		recordBudgets(string(r.Mode), node.Name(), string(node.Role()), updated.Status.Budgets)
	}

	// Only a projecting plane has members to report, and only it should clear
	// the field: a workload-mode manager writing nil here would erase what the
	// management plane published about the same node name.
	if r.Project.Enabled() {
		updated.Status.Clusters = clusterStatuses(onClusters, current.Status.Clusters)
		// The management plane's own budget figures, which no backend can give
		// it: it runs no Kueue and materializes nothing, so its numbers are the
		// fleet's members added up.
		updated.Status.Budgets = fleetBudgets(node.Quota.Spec.Budgets, onClusters, rolled)
	}

	violation, isFrozen := frozen[node.Name()]
	setConditions(&updated.Status, violation, isFrozen)
	if r.Materialize.Enabled() {
		setMaterializationStatus(&updated.Status, current.Generation, outcome, isFrozen, metav1.Now())
		updated.Status.SourceGeneration = materializedSourceGeneration(current, updated.Status)
	}

	if equalStatus(current.Status, updated.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, updated, client.MergeFrom(current)); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// The node was deleted or rewritten mid-pass; the next reconcile
			// sees the newer state. Neither is worth failing the pass over.
			return nil
		}
		return err
	}
	return nil
}

// setConditions stamps Ready and Degraded as a mutually exclusive pair, so a
// reader never has to reconcile two conditions that disagree.
//
// A node frozen by an ancestor reports the ancestor's cause rather than a
// generic message: the operator has to fix the ancestor, and saying so is the
// difference between a two-minute fix and a hunt.
func setConditions(status *v1beta1.AcceleratorQuotaStatus, violation tree.Violation, isFrozen bool) {
	now := metav1.Now()
	ready := metav1.Condition{
		Type:               v1beta1.AcceleratorQuotaReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.AcceleratorQuotaReasonAdmitted,
		Message:            "node is a valid member of the quota tree",
		LastTransitionTime: now,
		ObservedGeneration: status.ObservedGeneration,
	}
	degraded := metav1.Condition{
		Type:               v1beta1.AcceleratorQuotaDegraded,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.AcceleratorQuotaReasonAdmitted,
		Message:            "no invariant violated",
		LastTransitionTime: now,
		ObservedGeneration: status.ObservedGeneration,
	}

	if isFrozen {
		ready.Status = metav1.ConditionFalse
		ready.Reason = violation.Reason
		ready.Message = violation.Message
		degraded.Status = metav1.ConditionTrue
		degraded.Reason = violation.Reason
		degraded.Message = violation.Message
	}

	setCondition(status, ready)
	setCondition(status, degraded)
}

// setCondition is the one place conditions are written, so every condition this
// controller owns gets the same treatment.
func setCondition(status *v1beta1.AcceleratorQuotaStatus, condition metav1.Condition) {
	apimeta.SetStatusCondition(&status.Conditions, condition)
}

// equalStatus compares the fields this controller owns. Condition timestamps are
// excluded deliberately: SetStatusCondition only moves LastTransitionTime when
// the status actually flips, so comparing the rest is what makes a no-op pass a
// genuine no-op.
// materializedSourceGeneration echoes the generation the management plane
// stamped on a projection, but only once this pass actually materialized it.
//
// The number is the only thing that makes remote progress comparable: a
// projection's own generation is object-local and says nothing about which
// source revision it reflects. Echoing it before the objects were written would
// report progress that has not happened -- so a node that is frozen, or that
// failed to materialize, keeps whatever it last earned rather than advancing.
//
// Empty on a node nobody projected: an admin-authored node has no source
// elsewhere, and a number invented here would claim otherwise.
func materializedSourceGeneration(current *v1beta1.AcceleratorQuota,
	status v1beta1.AcceleratorQuotaStatus,
) int64 {
	stamped := current.Annotations[v1beta1.AcceleratorQuotaSourceGenerationAnnotation]
	if stamped == "" {
		return 0
	}
	if !apimeta.IsStatusConditionTrue(status.Conditions, v1beta1.AcceleratorQuotaMaterialized) {
		return current.Status.SourceGeneration
	}
	// A projector writes this annotation, so an unparsable value means someone
	// edited it by hand. Holding the previous number is more honest than
	// reporting zero, which reads as "materialized nothing yet".
	n, err := strconv.ParseInt(stamped, 10, 64)
	if err != nil {
		return current.Status.SourceGeneration
	}
	return n
}

// clusterStatuses renders one node's per-member rows, sorted so two passes over
// the same fleet produce the same object.
//
// A member that reported nothing this pass keeps the generations it last
// earned. The alternative -- zeroing them -- would read as "this member has
// materialized nothing", which is a much stronger claim than "we could not ask
// it", and the two call for opposite responses from an operator.
func clusterStatuses(observed map[string]clusterState,
	previous []v1beta1.AcceleratorQuotaClusterStatus,
) []v1beta1.AcceleratorQuotaClusterStatus {
	if len(observed) == 0 {
		return previous
	}
	was := make(map[string]v1beta1.AcceleratorQuotaClusterStatus, len(previous))
	for _, p := range previous {
		was[p.Cluster] = p
	}

	out := make([]v1beta1.AcceleratorQuotaClusterStatus, 0, len(observed))
	for cluster, st := range observed {
		row := v1beta1.AcceleratorQuotaClusterStatus{
			Cluster:                cluster,
			AppliedGeneration:      st.appliedGeneration,
			AppliedTime:            st.appliedTime,
			MaterializedGeneration: st.materializedGeneration,
			Message:                st.message,
		}
		if prev, ok := was[cluster]; ok {
			if row.AppliedGeneration == 0 {
				row.AppliedGeneration = prev.AppliedGeneration
				row.AppliedTime = prev.AppliedTime
			}
			if row.MaterializedGeneration == 0 {
				row.MaterializedGeneration = prev.MaterializedGeneration
			}
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cluster < out[j].Cluster })
	return out
}

// fleetLeafUsage reshapes the per-cluster view into what the roll expects: one
// entry per node, summed across the members holding it.
//
// Summing across CLUSTERS is not the same as summing across children, which the
// roll refuses to do for borrowed. Borrowing is cluster-local -- a Kueue cohort
// lives on one member -- so two members each lending to the same tenant are two
// separate loans, and the fleet really is running that much above its shares.
func fleetLeafUsage(fleet fleetView) map[string]map[usage.Key]usage.Observed {
	if len(fleet) == 0 {
		return nil
	}
	out := map[string]map[usage.Key]usage.Observed{}
	for node, byCluster := range fleet {
		for _, st := range byCluster {
			for _, cb := range st.budgets {
				if cb.resourceName == "" {
					continue
				}
				if out[node] == nil {
					out[node] = map[usage.Key]usage.Observed{}
				}
				k := usage.Key{ResourceName: cb.resourceName, ResourceFlavor: cb.resourceFlavor}
				agg := out[node][k]
				agg.Admitted.Add(cb.admitted)
				agg.Reserved.Add(cb.reserved)
				agg.Borrowed.Add(cb.borrowed)
				out[node][k] = agg
			}
		}
	}
	return out
}

// fleetBudgets is the hub's view of one node's budgets: what the admin
// authorized, and what the fleet is doing with it.
//
// Nominal is the authored total rather than the sum of the shares. They agree
// when every member reported, and when one did not the split is held -- so
// summing would quietly report a smaller fleet total for as long as a member
// was missing.
// The authored number is what the admin wrote and stays what the hub reports.
//
// The consumption figures ARE sums, because consumption is a fact about the
// fleet: work admitted on one member is work the fleet is running. Borrowed
// sums too, and means the same thing one tier up -- how much of what is running
// sits above the share that authorized it.
func fleetBudgets(authored []v1beta1.AcceleratorBudget,
	onClusters map[string]clusterState, rolled map[usage.Key]usage.Total,
) []v1beta1.AcceleratorBudgetStatus {
	if len(authored) == 0 {
		return nil
	}
	out := make([]v1beta1.AcceleratorBudgetStatus, 0, len(authored))
	for _, b := range authored {
		key := budgetKey(b.ResourceName, b.ResourceFlavor)
		row := v1beta1.AcceleratorBudgetStatus{
			ResourceName:   b.ResourceName,
			ResourceFlavor: b.ResourceFlavor,
			Nominal:        b.Nominal.DeepCopy(),
		}
		// The breakdown is this node's own, so only a leaf has one: a share is
		// apportioned to a leaf, and an ancestor is projected budget-less. An
		// ancestor with per-cluster rows would have to invent a nominal nobody
		// assigned it.
		for cluster, st := range onClusters {
			cb, ok := st.budgets[key]
			if !ok {
				continue
			}
			row.PerCluster = append(row.PerCluster, v1beta1.AcceleratorClusterBudgetStatus{
				Cluster:  cluster,
				Nominal:  cb.nominal.DeepCopy(),
				Admitted: cb.admitted.DeepCopy(),
				Reserved: cb.reserved.DeepCopy(),
				Borrowed: cb.borrowed.DeepCopy(),
			})
		}
		sort.Slice(row.PerCluster, func(i, j int) bool {
			return row.PerCluster[i].Cluster < row.PerCluster[j].Cluster
		})

		// The totals come from the roll, which is what carries a leaf's figures
		// up to the tiers above it.
		if t, ok := rolled[usage.Key{ResourceName: b.ResourceName, ResourceFlavor: b.ResourceFlavor}]; ok {
			row.Admitted = t.Admitted.DeepCopy()
			row.Reserved = t.Reserved.DeepCopy()
			row.Borrowed = t.Borrowed.DeepCopy()
		}
		out = append(out, row)
	}
	return out
}

// equalPerCluster compares one budget's member rows.
func equalPerCluster(a, b []v1beta1.AcceleratorClusterBudgetStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Cluster != b[i].Cluster ||
			a[i].Nominal.Cmp(b[i].Nominal) != 0 ||
			a[i].Admitted.Cmp(b[i].Admitted) != 0 ||
			a[i].Reserved.Cmp(b[i].Reserved) != 0 ||
			a[i].Borrowed.Cmp(b[i].Borrowed) != 0 {
			return false
		}
	}
	return true
}

// equalClusters compares the per-member rows, excluding AppliedTime. Including
// it would make every successful pass a write, since a clean re-apply restamps
// the time whether or not anything moved -- and this status is watched, so a
// write per pass is a reconcile per pass forever.
func equalClusters(a, b []v1beta1.AcceleratorQuotaClusterStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Cluster != b[i].Cluster ||
			a[i].AppliedGeneration != b[i].AppliedGeneration ||
			a[i].MaterializedGeneration != b[i].MaterializedGeneration ||
			a[i].Message != b[i].Message {
			return false
		}
	}
	return true
}

func equalStatus(a, b v1beta1.AcceleratorQuotaStatus) bool {
	if a.ObservedGeneration != b.ObservedGeneration || a.Parent != b.Parent {
		return false
	}
	if a.SourceGeneration != b.SourceGeneration {
		return false
	}

	if !equalMaterialization(a.Materialization, b.Materialization) {
		return false
	}
	if !equalBudgets(a.Budgets, b.Budgets) {
		return false
	}
	if !equalClusters(a.Clusters, b.Clusters) {
		return false
	}
	for _, condType := range []string{
		v1beta1.AcceleratorQuotaReady,
		v1beta1.AcceleratorQuotaDegraded,
		v1beta1.AcceleratorQuotaMaterialized,
	} {
		ca := apimeta.FindStatusCondition(a.Conditions, condType)
		cb := apimeta.FindStatusCondition(b.Conditions, condType)
		if ca == nil || cb == nil {
			if ca != cb {
				return false
			}
			continue
		}
		if ca.Status != cb.Status || ca.Reason != cb.Reason || ca.Message != cb.Message ||
			ca.ObservedGeneration != cb.ObservedGeneration {
			return false
		}
	}
	return true
}

// SetupWithManager wires the reconciler to watch AcceleratorQuotas.
//
// Every event enqueues the same key, because the unit of work is the tree, not
// the object: editing one node can change another's verdict, and a per-object
// queue would leave the second node reporting a stale condition until it
// happened to be touched. Collapsing to one key also means a burst of edits
// coalesces into a single rebuild.
// WithTransportEvents wires the channel the Connector signals when the set of
// reachable members moves, so a fleet that becomes whole again is re-split at
// once rather than on the next resync tick. Without it the controller is
// poll-only, which is correct but slow to recover.
func WithTransportEvents(ch <-chan event.GenericEvent) Option {
	return func(o *options) { o.transportEvents = ch }
}

// WithMemberEvents wires the channel a status funnel fills when an
// AcceleratorQuota changes on any member, so the hub's account of the fleet
// follows the fleet instead of a clock.
//
// Consumption moves without anything in the tree changing, and none of this
// controller's own triggers notice: it is woken by edits to the hub's own
// nodes and by its resync tick, neither of which fires when a tenant's work
// starts or finishes on a member. The workload plane has the same problem one
// layer down and solves it the same way, by watching the objects that carry the
// figures rather than re-reading them on a timer.
//
// This does not feed back on itself. The hub's own applies to a member are
// server-side applies, and one that changes nothing does not bump
// resourceVersion, so it raises no event to wake the pass that made it.
func WithMemberEvents(ch <-chan event.GenericEvent) Option {
	return func(o *options) { o.memberEvents = ch }
}

// Option configures SetupWithManager.
type Option func(*options)

type options struct {
	transportEvents <-chan event.GenericEvent
	memberEvents    <-chan event.GenericEvent
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts ...Option) error {
	var cfg options
	for _, o := range opts {
		o(&cfg)
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.APIReader == nil {
		return fmt.Errorf("acceleratorquota: APIReader is required (the tree verdict must not be read from a cache)")
	}
	if r.Options.RootName == "" {
		return fmt.Errorf("acceleratorquota: %w", tree.ErrRootNameUnset)
	}
	b := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.AcceleratorQuota{}).
		Named("acceleratorquota")

	if r.Capacity.Enabled() {
		// Reconciles are event-driven, and a cluster can start with no
		// AcceleratorQuota and no Nodes — nothing to fire an event, so nothing
		// would ever create the root and capacity would never be reported. One
		// pass at startup removes that dependency on the cluster happening to
		// contain something.
		//
		// A plain RunnableFunc is leader-elected, so a standby replica does not
		// race the leader; AlreadyExists is benign in any case. The pass retries
		// internally and never returns an error, because the write goes through
		// this component's own fail-closed webhook and the manager it would take
		// down is the one that brings that webhook up.
		if err := mgr.Add(manager.RunnableFunc(r.bootstrapRoot)); err != nil {
			return fmt.Errorf("acceleratorquota: scheduling the root check: %w", err)
		}

		// Every node maps to the same request: reconciliation is whole-tree, so
		// which node changed does not narrow the work.
		//
		// The predicate is not an optimisation to revisit later. Node status is
		// among the highest-volume writes in a cluster and almost none of it
		// moves a number this controller reads, so without it a large fleet
		// would rebuild the tree continuously.
		b = b.WatchesRawSource(source.Kind(
			mgr.GetCache(),
			&corev1.Node{},
			handler.TypedEnqueueRequestsFromMapFunc(func(context.Context, *corev1.Node) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: r.Options.RootName}}}
			}),
			predicate.TypedFuncs[*corev1.Node]{
				UpdateFunc: func(e event.TypedUpdateEvent[*corev1.Node]) bool {
					return nodeCapacityChanged(e.ObjectOld, e.ObjectNew, r.Capacity.Resources)
				},
			},
		))
	}

	// Consumption moves without anything in the tree changing, and nothing else
	// here would notice: the reconcile is woken by AcceleratorQuota edits and by
	// node capacity, neither of which a workload admission touches. Left to the
	// resync tick alone, a queue's figures are up to a full interval stale — long
	// enough that a fleet with work running reads as an idle one.
	//
	// A backend that cannot say what to watch simply does not get watched, and
	// its figures refresh on the resync tick as before.
	//
	// Every event maps to one request, as the node watch does, so a burst of
	// admissions collapses into a single queued pass rather than one per
	// workload.
	if watcher, ok := r.Materialize.Backend.(backend.UsageWatcher); ok {
		obj, usageChanged := watcher.WatchUsage()
		b = b.Watches(obj,
			handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: r.Options.RootName}}}
			}),
			builder.WithPredicates(usageChanged),
		)
	}
	// One handler for both, because the pass they trigger is the same one: it
	// rebuilds the whole tree from the root, so a burst of member events and a
	// reconnection collapse into a single queued request rather than one each.
	wake := handler.EnqueueRequestsFromMapFunc(
		func(_ context.Context, obj client.Object) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: obj.GetName()}}}
		})
	if cfg.memberEvents != nil {
		b = b.WatchesRawSource(source.Channel(cfg.memberEvents, wake))
	}
	if cfg.transportEvents != nil {
		// Every event names the root and the pass rebuilds the whole tree, so a
		// burst of reconnections collapses into one queued pass rather than one
		// per member.
		b = b.WatchesRawSource(source.Channel(cfg.transportEvents, wake))
	}
	return b.Complete(r)
}
